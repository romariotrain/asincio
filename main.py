import requests
import asyncio
import aiohttp
import datetime
from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople




CHUNK_SIZE = 10


async def get_people(session, people_id):
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
        return json_data


def get_homeworld(item):
    response = requests.get(item['homeworld'])
    return response.json()['name']


async def get_films(session, item):
    films_titles = ''
    for i in item['films']:
        async with session.get(i) as response:
            json_data = await response.json()
            if films_titles == '':
                films_titles = films_titles + json_data['title']
            else:
                films_titles = films_titles + ', ' + json_data['title']
    return films_titles


async def get_vehicles(session, item):
    vehicles_titles = ''
    for i in item['vehicles']:
        async with session.get(i) as response:
            json_data = await response.json()
            if vehicles_titles == '':
                vehicles_titles = vehicles_titles + json_data['name']
            else:
                vehicles_titles = vehicles_titles + ', ' + json_data['name']
    return vehicles_titles


async def get_species(session, item):
    species_titles = ''
    for i in item['species']:
        async with session.get(i) as response:
            json_data = await response.json()
            if species_titles == '':
                species_titles = species_titles + json_data['name']
            else:
                species_titles = species_titles + ', ' + json_data['name']
    return species_titles


async def get_starships(session, item):
    starships_titles = ''
    for i in item['starships']:
        async with session.get(i) as response:
            json_data = await response.json()
            if starships_titles == '':
                starships_titles = starships_titles + json_data['name']
            else:
                starships_titles = starships_titles + ', ' + json_data['name']
    return starships_titles


async def paste_to_db(results):

    for item in results:

        films = await get_films(session, item)
        species = await get_species(session, item)
        vehicles = await get_vehicles(session, item)
        starships = await get_starships(session, item)

        swapi_people = [SwapiPeople(birth_year=item['birth_year'],
                        eye_color=item['eye_color'], films=films, gender=item['gender'], hair_color=item['hair_color'],
                        height=item['height'], homeworld=get_homeworld(item),
                        mass= item['mass'], name=item['name'], skin_color=item['skin_color'],
                        species=species, starships=starships, vehicles=vehicles)]
        async with Session() as session:
            session.add_all(swapi_people)
            await session.commit()



async def main():
    start = datetime.datetime.now()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session = aiohttp.ClientSession()
    coros = (get_people(session, i) for i in range(1, 11))
    for coros_chunk in chunked(coros, CHUNK_SIZE):
        results = await asyncio.gather(*coros_chunk)
        print(results)
        asyncio.create_task(paste_to_db(results))
    set_tasks = asyncio.all_tasks()
    for task in set_tasks:
        if task != asyncio.current_task():
            await task
    await session.close()

    print(datetime.datetime.now() - start)


asyncio.run(main())
