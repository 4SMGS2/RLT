import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta
import bson
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message

def aggregate(dt_from, dt_upto, group_type):
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    with open('sample_collection.bson', 'rb') as bson_file:
        data = bson.decode_all(bson_file.read())

    if group_type == "hour":
        interval = timedelta(hours=1)
        date_format = "%Y-%m-%dT%H:00:00"
    elif group_type == "day":
        interval = timedelta(days=1)
        date_format = "%Y-%m-%dT00:00:00"
    elif group_type == "month":
        interval = timedelta(days=30)
        date_format = "%Y-%m-01T00:00:00"

    aggregated_dt = {}

    current_dt = dt_from
    while current_dt <= dt_upto:
        aggregated_dt[current_dt.strftime(date_format)] = 0
        current_dt += interval

    for record in data:
        record_dt = record['dt']
        if dt_from <= record_dt <= dt_upto:
            if group_type == "hour":
                group_key = record_dt.replace(minute=0, second=0, microsecond=0)
            elif group_type == "day":
                group_key = record_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            elif group_type == "month":
                group_key = record_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            aggregated_dt[group_key.strftime(date_format)] += record['value']

    dataset = list(aggregated_dt.values())
    labels = list(aggregated_dt.keys())

    return {"dataset": dataset, "labels": labels}

TOKEN = "6784073202:AAEmqqNPe_Puf3z09pHJla631YCyEy_9FpA"

dp = Dispatcher()

@dp.message()
async def echo_handler(message: Message) -> None:
    data = json.loads(message.text)
    dt_from = data['dt_from']
    dt_to = data['dt_upto']
    group_type = data['group_type']
    result = aggregate(dt_from, dt_to, group_type)
    result = json.dumps(result)
    await (message.answer(result))


async def main() -> None:
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())