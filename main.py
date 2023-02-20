import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from _321CQU.sql_helper.SqlManager import DatabaseConfig
from micro_services_protobuf.protobuf_enum.notification_center import NotificationEvent

from utils.SqlManager import SqlManager
from score_query import send_notification, get_new_scores, CQUSession


async def ios_score_query():
    print("发送iOS用户通知")
    async with SqlManager().cursor(DatabaseConfig.Notification) as cursor:
        await cursor.execute('select uid from Subscribe where event_id = %s and '
                             'exists(select uid from Apns where Subscribe.uid = Apns.uid)',
                             (NotificationEvent.ScoreQuery.event_id,))
        uids = list(await cursor.fetchall())

    for uid in uids:
        scores = await get_new_scores(uid, CQUSession(year=2022, is_autumn=True))
        await send_notification(uid, scores)


async def wechat_score_query():
    print("发送微信小程序成绩通知")
    async with SqlManager().cursor(DatabaseConfig.Notification) as cursor:
        await cursor.execute('select uid from Subscribe where event_id = %s and '
                             'exists(select uid from Openid where Subscribe.uid = Openid.uid) and '
                             'not exists(select uid from Apns where Subscribe.uid = Apns.uid)',
                             (NotificationEvent.ScoreQuery.event_id,))
        uids = list(await cursor.fetchall())

    for uid in uids:
        scores = await get_new_scores(uid, CQUSession(year=2022, is_autumn=True))
        await send_notification(uid, scores)


if __name__ == '__main__':
    scheduler = AsyncIOScheduler()
    scheduler.add_job(ios_score_query, 'interval', hours=1)
    scheduler.add_job(wechat_score_query, 'interval', hours=2)
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
        # asyncio.new_event_loop().run_until_complete(wechat_score_query())
    except (KeyboardInterrupt, SystemExit):
        pass
