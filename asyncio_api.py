import asyncio
import aiohttp
import datetime
from pprint import pprint

import requests

from models import Base, SwapiPeople, Session, engine
from more_itertools import chunked



MAX_CHUNK_SIZE = 5


# Выгрузка персонажей из SWAPI
async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{people_id}")
    json_data = await response.json()

    if 'detail' not in json_data:

        homeworld_url = json_data['homeworld']
        async with session.get(f'{homeworld_url}') as resp:
            json_item = await resp.json()
            homeworld = json_item['name']

        films_list, species_list, starships_list, vehicles_list = [], [], [], []

        for film in json_data['films']:
            async with session.get(f'{film}') as resp:
                json_item = await resp.json()
                films_list.append(json_item['title'])

        for species in json_data['species']:
            async with session.get(f'{species}') as resp:
                json_species = await resp.json()
                species_list.append(json_species['name'])

        for starship in json_data['starships']:
            async with session.get(f'{starship}') as resp:
                json_starships = await resp.json()
                starships_list.append(json_starships['name'])

        for vehicle in json_data['vehicles']:
            async with session.get(f'{vehicle}') as resp:
                json_vehicles = await resp.json()
                vehicles_list.append(json_vehicles['name'])

        add_data = {
            'id': people_id,
            'films': ",".join(films_list),
            'species': ",".join(species_list),
            'starships': ",".join(starships_list),
            'vehicles': ",".join(vehicles_list),
            'homeworld': homeworld,
        }
        json_data.update(add_data)
    await session.close()
    return json_data


async def insert_to_db(people_json_list):
    # pprint(people_json_list)
    async with Session() as session:

        swapi_people_list = [SwapiPeople(
            name=json_data['name'],
            person_id=json_data['id'],
            birth_year=json_data['birth_year'],
            eye_color=json_data['eye_color'],
            films=json_data['films'],
            gender=json_data['gender'],
            hair_color=json_data['hair_color'],
            height=json_data['height'],
            homeworld=json_data['homeworld'],
            mass=json_data['mass'],
            skin_color=json_data['skin_color'],
            species=json_data['species'],
            starships=json_data['starships'],
            vehicles=json_data['vehicles'],
        ) for json_data in people_json_list]
        session.add_all(swapi_people_list)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    for ids_chunk in chunked(range(1, 100), MAX_CHUNK_SIZE):

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

