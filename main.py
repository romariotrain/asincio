
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


async def get_homeworld(session, item):
    async with session.get(item['homeworld']) as response:
        json_data = await response.json()
    return json_data['name']


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


async def get_data1(item, client_session):
    data1 = {}
    data1['films'] = await get_films(client_session, item)
    data1['species'] = await get_species(client_session, item)
    data1['vehicles'] = await get_vehicles(client_session, item)
    data1['starships'] = await get_starships(client_session, item)
    data1['homeworld'] = await get_homeworld(client_session, item)
    data1['birth_year'] = item['birth_year']
    data1['eye_color'] = item['eye_color']
    data1['gender'] = item['gender']
    data1['hair_color'] = item['hair_color']
    data1['height'] = item['height']
    data1['mass'] = item['mass']
    data1['name'] = item['name']
    data1['skin_color'] = item['skin_color']
    return data1


async def paste_to_db(data2):

        eye_color = data2['eye_color']
        gender = data2['gender']
        birth_year = data2['birth_year']
        hair_color = data2['hair_color']
        height = data2['height']
        mass = data2['mass']
        name = data2['name']
        skin_color = data2['skin_color']
        homeworld = data2['homeworld']
        species = data2['species']
        starships = data2['starships']
        vehicles = data2['vehicles']
        films = data2['films']

        swapi_people = [SwapiPeople(birth_year=birth_year,
                        eye_color=eye_color, films=films, gender=gender, hair_color=hair_color,
                        height=height, homeworld=homeworld,
                        mass=mass, name=name, skin_color=skin_color,
                        species=species, starships=starships, vehicles=vehicles)]
        async with Session() as session:
            session.add_all(swapi_people)
            await session.commit()


async def main():
    start = datetime.datetime.now()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session = aiohttp.ClientSession()
    coros = (get_people(session, i) for i in range(1, 83))
    for coros_chunk in chunked(coros, CHUNK_SIZE):
        results = await asyncio.gather(*coros_chunk)
        for item in results:
            if len(item) != 1:
                data2 = await get_data1(item, session)
                asyncio.create_task(paste_to_db(data2))
    #
    await session.close()
    set_tasks = asyncio.all_tasks()
    for task in set_tasks:
        if task != asyncio.current_task():
            await task

    print(datetime.datetime.now() - start)


asyncio.run(main())
