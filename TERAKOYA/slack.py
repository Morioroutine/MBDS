import os
import time
from collections import Counter
from datetime import datetime
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
client = WebClient(token=SLACK_BOT_TOKEN)
OUTPUT_CHANNEL_ID = "C0213EHETCG"  # terakoyabuddy_全員

# 対象チャンネル名フィルター
def get_target_channels():
    channels = []
    cursor = None
    while True:
        try:
            response = client.conversations_list(cursor=cursor, limit=200)
            for channel in response['channels']:
                if not channel['is_archived'] and channel['name'].startswith("buddy第25期_"):
                    channels.append({
                        "id": channel['id'],
                        "name": channel['name']
                    })
            cursor = response.get('response_metadata', {}).get('next_cursor')
            if not cursor:
                break
            time.sleep(1)
        except SlackApiError as e:
            print("❌ チャンネル取得エラー:", e.response["error"])
            break
    return pd.DataFrame(channels)

# ユーザー情報取得
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
            time.sleep(1)
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
            time.sleep(1)
        except SlackApiError as e:
            print(f"❌ メッセージ取得失敗 (channel: {channel_id}) → {e.response['error']}")
            break
    return messages

# メッセージ整形
def process_messages(messages, channel_id, channel_name):
    data = []
    for msg in messages:
        if "user" in msg:
            ts = float(msg['ts'])
            date = datetime.fromtimestamp(ts).date()
            data.append({
                "user_id": msg['user'],
                "channel_id": channel_id,
                "channel_name": channel_name,
                "date": date,
                "reactions": msg.get("reactions", [])
            })
    return pd.DataFrame(data)

# メイン処理
def main():
    user_df = get_user_map()
    user_map = dict(zip(user_df['user_id'], user_df['display_name']))
    channel_df = get_target_channels()

    print(f"✅ 対象チャンネル数: {len(channel_df)}")

    all_messages = []
    for _, row in channel_df.iterrows():
        print(f"📥 {row['name']} のメッセージを取得中...")
        msgs = fetch_messages(row['id'])
        if msgs:
            df = process_messages(msgs, row['id'], row['name'])
            all_messages.append(df)

    if not all_messages:
        print("❌ メッセージ取得に失敗（Bot未参加の可能性）")
        return

    messages_df = pd.concat(all_messages, ignore_index=True)
    if messages_df.empty:
        print("⚠️ メッセージは取得されましたが、空のようです。")
        return

    messages_df = messages_df.merge(user_df, on="user_id", how="left")

    # 投稿数集計
    top_users = messages_df.groupby("display_name").size().reset_index(name="post_count").sort_values("post_count", ascending=False).head(20)
    top_channels = messages_df.groupby("channel_name").size().reset_index(name="post_count").sort_values("post_count", ascending=False).head(20)

    # リアクション集計
    reaction_counter = Counter()
    for _, row in messages_df.iterrows():
        for reaction in row["reactions"]:
            for uid in reaction.get("users", []):
                reaction_counter[uid] += 1

    reaction_df = pd.DataFrame(reaction_counter.items(), columns=["user_id", "reaction_count"])
    reaction_df["display_name"] = reaction_df["user_id"].map(user_map)
    reaction_df = reaction_df[["display_name", "reaction_count"]].sort_values("reaction_count", ascending=False).head(20)

    # メッセージ生成
    message = "*📊 投稿数 & スタンプランキング TOP20（buddy第25期_ 限定）*\n\n"

    message += "*👤 ユーザー別 投稿数 Top 20:*\n"
    for _, row in top_users.iterrows():
        name = row['display_name'] or "Unknown"
        message += f"- {name}: {row['post_count']}件\n"

    message += "\n*📺 チャンネル別 投稿数 Top 20:*\n"
    for _, row in top_channels.iterrows():
        message += f"- {row['channel_name']}: {row['post_count']}件\n"

    message += "\n👍 ユーザー別 スタンプを押した回数 Top 20:\n"
    for _, row in reaction_df.iterrows():
        name = row['display_name'] or "Unknown"
        message += f"- {name}: {row['reaction_count']}回\n"

    # Slackに投稿
    try:
        client.chat_postMessage(
            channel=OUTPUT_CHANNEL_ID,
            text=message
        )
        print("✅ Slackに投稿しました！")
    except SlackApiError as e:
        print("❌ Slack投稿失敗:", e.response["error"])

if __name__ == "__main__":
    main()
