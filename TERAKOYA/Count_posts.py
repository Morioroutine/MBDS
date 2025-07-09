import os
import time
from datetime import datetime
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

# .envからSLACK_BOT_TOKENを読み込む
load_dotenv()
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
client = WebClient(token=SLACK_BOT_TOKEN)

# チャンネル一覧取得（buddy第25期_ で始まるアクティブなチャンネル）
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
            time.sleep(1)
        except SlackApiError as e:
            print("❌ ユーザー取得エラー:", e.response["error"])
            break
    return pd.DataFrame(users)

# メッセージ整形
def process_messages(messages, channel_id, channel_name):
    data = []
    for msg in messages:
        if "user" in msg:
            try:
                ts = float(msg['ts'])
                date = datetime.fromtimestamp(ts).date()
                data.append({
                    "user_id": msg['user'],
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "date": date
                })
            except Exception as e:
                print(f"⚠️ 日付変換エラー: {e} → msg: {msg}")
        else:
            print(f"⚠️ userキーなし: {msg}")
    return pd.DataFrame(data)

# メイン処理
def main():
    user_df = get_user_map()
    channel_df = get_target_channels()

    print(f"✅ 対象チャンネル数: {len(channel_df)}")

    parent_messages = []
    all_messages = []

    for _, row in channel_df.iterrows():
        print(f"📥 {row['name']} のメッセージを取得中...")
        try:
            # 親メッセージの取得
            cursor = None
            while True:
                response = client.conversations_history(channel=row['id'], cursor=cursor, limit=200)
                parents = response['messages']

                # 親メッセージを保存
                parent_df = process_messages(parents, row['id'], row['name'])
                parent_messages.append(parent_df)

                # 親 + リプライをまとめる
                combined = parents.copy()
                for msg in parents:
                    if msg.get("reply_count", 0) > 0 and (msg.get("thread_ts") is None or msg["ts"] == msg["thread_ts"]):
                        try:
                            replies = client.conversations_replies(channel=row['id'], ts=msg['ts'])['messages'][1:]
                            combined.extend(replies)
                        except SlackApiError as e:
                            print(f"⚠️ リプライ取得失敗 ({row['name']}) → {e.response['error']}")

                all_df = process_messages(combined, row['id'], row['name'])
                all_messages.append(all_df)

                cursor = response.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
                time.sleep(1)

        except SlackApiError as e:
            print(f"❌ メッセージ取得失敗 ({row['name']}) → {e.response['error']}")

    if not all_messages or not parent_messages:
        print("❌ メッセージ取得に失敗または空です。")
        return

    parent_df = pd.concat(parent_messages, ignore_index=True)
    all_df = pd.concat(all_messages, ignore_index=True)

    # 日付フィルター：2025年5月〜6月
    start_date = datetime(2025, 5, 1).date()
    end_date = datetime(2025, 6, 30).date()
    parent_df = parent_df[(parent_df["date"] >= start_date) & (parent_df["date"] <= end_date)]
    all_df = all_df[(all_df["date"] >= start_date) & (all_df["date"] <= end_date)]

    # display_name 結合
    parent_df = parent_df.merge(user_df, on="user_id", how="left")
    all_df = all_df.merge(user_df, on="user_id", how="left")

    # 集計
    active_days = parent_df.groupby("display_name")["date"].nunique().reset_index(name="active_days")
    total_posts = parent_df.groupby("display_name").size().reset_index(name="total_posts")
    posts_reply_included = all_df.groupby("display_name").size().reset_index(name="posts_reply_included")

    # 統合
    result_df = active_days.merge(total_posts, on="display_name", how="outer") \
                           .merge(posts_reply_included, on="display_name", how="outer")
    result_df = result_df.fillna(0).astype({
        "active_days": int,
        "total_posts": int,
        "posts_reply_included": int
    })
    result_df = result_df.sort_values(by="active_days", ascending=False)

    # 保存
    print("\n📊 投稿統計（5月〜6月、リプライ含む）:")
    print(result_df)
    result_df.to_csv("slack_post_summary_with_replies.csv", index=False, encoding="utf-8-sig")
    print("✅ 結果を 'slack_post_summary_with_replies.csv' に保存しました")

if __name__ == "__main__":
    main()
