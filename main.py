import asyncio
import datetime
from typing import List

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from _321CQU.sql_helper.SqlManager import DatabaseConfig
from micro_services_protobuf.protobuf_enum.notification_center import NotificationEvent
from micro_services_protobuf.model.cqu_session import CQUSession

from utils.SqlManager import SqlManager
from utils.ConfigReader import ConfigReader
from utils.log_config import info_logger
from score_query import score_query


async def ios_score_query():
    info_logger.info('开始iOS成绩通知')

    task_num = int(ConfigReader().get_config('ScoreQuerySetting', 'task_num'))
    async with SqlManager().cursor(DatabaseConfig.Notification) as cursor:
        await cursor.execute('select uid from Subscribe where event_id = %s and '
                             'exists(select uid from Apns where Subscribe.uid = Apns.uid)',
                             (NotificationEvent.ScoreQuery.event_id,))
        uids: List[bytes] = list(map(lambda x: x[0], await cursor.fetchall()))

    for i in range(0, len(uids), task_num):
        tasks = []
        for j in range(min(task_num, (len(uids) - i))):
            info_logger.info(f"查询iOS用户: {uids[i + j].hex()}成绩")
            tasks.append(score_query(uids[i + j], CQUSession(year=2022, is_autumn=True)))

        await asyncio.gather(*tasks)

    info_logger.info("结束iOS成绩通知")


async def wechat_score_query():
    info_logger.info("开始小程序成绩通知")

    task_num = int(ConfigReader().get_config('ScoreQuerySetting', 'task_num'))
    async with SqlManager().cursor(DatabaseConfig.Notification) as cursor:
        await cursor.execute('select uid from Subscribe where event_id = %s and '
                             'exists(select uid from Openid where Subscribe.uid = Openid.uid) and '
                             'not exists(select uid from Apns where Subscribe.uid = Apns.uid)',
                             (NotificationEvent.ScoreQuery.event_id,))
        uids: List[bytes] = list(map(lambda x: x[0], await cursor.fetchall()))

    for i in range(0, len(uids), task_num):
        tasks = []
        for j in range(min(task_num, (len(uids) - i))):
            info_logger.info(f"查询小程序用户: {uids[i + j].hex()}成绩")
            tasks.append(score_query(uids[i + j], CQUSession(year=2022, is_autumn=True)))

        await asyncio.gather(*tasks)
    info_logger.info("结束小程序成绩通知")


if __name__ == '__main__':
    scheduler = AsyncIOScheduler()
    scheduler.add_job(ios_score_query, 'interval', max_instances=2, next_run_time=datetime.datetime.now(), hours=1, jitter=30)
    scheduler.add_job(wechat_score_query, 'interval', max_instances=2, next_run_time=datetime.datetime.now(), hours=2, jitter=30)
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
        # asyncio.new_event_loop().run_until_complete(ios_score_query())
    except (KeyboardInterrupt, SystemExit):
        pass
