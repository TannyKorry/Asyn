import asyncio
import aiohttp
import datetime
from pprint import pprint
from models import Base, SwapiPeople, Session, engine
from more_itertools import chunked

MAX_CHUNK_SIZE = 10
# Выгрузка персонажей из SWAPI
async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def insert_to_db(people_json_list):
    async with Session() as session:
        swapi_people_list = [SwapiPeople(json=json_data) for json_data in people_json_list]
        session.add_all(swapi_people_list)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)


    # q = len(requests.get(f"https://swapi.dev/api/people").json()['results'])
    for ids_chunk in chunked(range(1, 84), MAX_CHUNK_SIZE):
        coros = [get_people(people_id) for people_id in ids_chunk]
        people_json_list = await asyncio.gather(*coros)
        asyncio.create_task(insert_to_db(people_json_list)) #вместо await, чтобы вставка в базу не блокировала получение следующего персонажа, будем использовать задачу
        current_task = asyncio.current_task() #получение текущей task
        tasks_set = asyncio.all_tasks()# возвращает множество задач, которые выполняются (в том числе и сама функция main будет считаться задачей)
        tasks_set.remove(current_task)  # исключение текущей task
        await asyncio.gather(*tasks_set)
    await engine.dispose()


start = datetime.datetime.now()
asyncio.run(main())
pprint(datetime.datetime.now() - start)
