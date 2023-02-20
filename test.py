import aiohttp
import asyncio
import requests



ok = [{'name': 'Luke Skywalker', 'height': '172', 'mass': '77', 'hair_color': 'blond', 'skin_color': 'fair', 'eye_color': 'blue', 'birth_year': '19BBY', 'gender': 'male', 'homeworld': 'https://swapi.dev/api/planets/1/', 'films': ['https://swapi.dev/api/films/1/', 'https://swapi.dev/api/films/2/', 'https://swapi.dev/api/films/3/', 'https://swapi.dev/api/films/6/']}]

def get_people(people_id):
    ok = requests.get(f'https://swapi.dev/api/people/{people_id}')
    return ok.json()

oki = [get_people(i) for i in range(1, 7)]

async def get_films(session, item):
    films = []
    item_films = []
    for i in item:
        item_films = i['films']
    for i in item_films:
        async with session.get(i) as response:
            json_data = await response.json()
            films.append(json_data)
    print(len(films))
    return films


async def get_films_titles():
    session = aiohttp.ClientSession()
    films = get_films(session, oki)
    result = await films

    films_titles = ''
    for i in result:
        # print(result)
        if films_titles == '':
            films_titles = films_titles + i['title']
        else:
            films_titles = films_titles + ', ' + i['title']
    # print(films_titles)
    await session.close()
    return films_titles


asyncio.run(get_films_titles())
print(len(oki))

# A New Hope, The Empire Strikes Back, Return of the Jedi, Revenge of the Sith