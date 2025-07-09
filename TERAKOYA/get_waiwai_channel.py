import os
import time
from datetime import datetime
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

# ===== 集計対象期間をここで設定 =====
TARGET_YEAR = 2025
TARGET_MONTH = 6
# =================================

# Slack設定
load_dotenv()
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
client = WebClient(token=SLACK_BOT_TOKEN)

# チャンネル一覧取得
def get_all_channels():
    channels = []
    cursor = None
    while True:
        try:
            response = client.conversations_list(cursor=cursor, limit=200)
            for channel in response['channels']:
                if not channel['is_archived']:
                    channels.append({"id": channel['id'], "name": channel['name']})
            cursor = response.get('response_metadata', {}).get('next_cursor')
            if not cursor:
                break
            time.sleep(2)
        except SlackApiError as e:
            print("❌ チャンネル取得失敗:", e.response["error"])
            break
    return pd.DataFrame(channels)

# ユーザー一覧取得
def get_user_map():
    users = []
    cursor = None
    while True:
        try:
            response = client.users_list(cursor=cursor, limit=200)
            for user in response['members']:
                if user.get("deleted", False) or user.get("is_bot", False):
                    continue
                users.append({
                    "user_id": user['id'],
                    "display_name": user['profile'].get('display_name') or user['profile'].get('real_name') or "Unknown"
                })
            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
            time.sleep(2)
        except SlackApiError as e:
            print("❌ ユーザー取得エラー:", e.response["error"])
            break
    return pd.DataFrame(users)

# メッセージ取得
def fetch_messages(channel_id):
    messages = []
    cursor = None
    while True:
        try:
            response = client.conversations_history(channel=channel_id, cursor=cursor, limit=200)
            messages.extend(response['messages'])
            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
            time.sleep(2)
        except SlackApiError as e:
            print(f"❌ メッセージ取得失敗 ({channel_id}):", e.response["error"])
            break
    return messages

# メイン処理
def main():
    print(f"🚀 集計開始（対象：{TARGET_YEAR}年{TARGET_MONTH}月）...")

    user_df = get_user_map()
    user_map = dict(zip(user_df['user_id'], user_df['display_name']))
    channels_df = get_all_channels()

    result_data = []

    for _, row in channels_df.iterrows():
        channel_id = row['id']
        channel_name = row['name']
        messages = fetch_messages(channel_id)
        if not messages:
            continue

        post_count = 0
        active_users = set()
        reaction_count = 0

        for msg in messages:
            if "user" not in msg or "ts" not in msg:
                continue

            ts = float(msg["ts"])
            msg_date = datetime.fromtimestamp(ts)
            if msg_date.year != TARGET_YEAR or msg_date.month != TARGET_MONTH:
                continue

            post_count += 1
            active_users.add(msg['user'])

            for reaction in msg.get("reactions", []):
                reaction_count += len(reaction.get("users", []))

        if post_count == 0 and reaction_count == 0:
            continue

        score = post_count + reaction_count + len(active_users) * 2
        result_data.append({
            "channel_name": channel_name,
            "posts": post_count,
            "reactions": reaction_count,
            "active_users": len(active_users),
            "score": score
        })

    result_df = pd.DataFrame(result_data)
    if result_df.empty:
        print("⚠️ 投稿のあるチャンネルが見つかりませんでした。")
        return

    result_df = result_df.sort_values("score", ascending=False).reset_index(drop=True).head(10)

    # 出力
    message = "====== テスト出力: チャンネルランキング ======\n"
    message += f"※集計期間: {TARGET_YEAR}年{TARGET_MONTH}月\n"
    message += "※集計方法: 投稿数 + リアクション数 + アクティブユーザー数×2\n"
    message += "\n*🔥 TERAKOYA盛り上がりチャンネル TOP10*\n"

    for idx, row in result_df.iterrows():
        message += (
            f"\n{idx+1}. #{row['channel_name']}\n"
            f"　📨 投稿数: {row['posts']}　🧑‍🤝‍🧑 アクティブユーザー: {row['active_users']}　👍 リアクション数: {row['reactions']}"
        )

    message += "\n\n===========================================\n"
    print(message)

if __name__ == "__main__":
    main()
