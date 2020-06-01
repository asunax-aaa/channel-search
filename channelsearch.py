from urllib.parse import urlencode
from itertools import zip_longest

import aiohttp
import asyncio
import math


def grouper(n, iterable, pad_value=None):
    return list(zip_longest(*[iter(iterable)]*n, fillvalue=pad_value))


class ChannelSearch:

    def __init__(self, auth, user_agent):
        """
        [auth] - Your discord authentication token, this is only for user accounts.
        [user_agent] Your discord client user agent.
        """

        self.auth = auth
        self.user_agent = user_agent
        self.session = aiohttp.ClientSession()

        self.headers = {
            'Authorization': auth,
            'User-Agent': user_agent,
            'Content-Type': 'application/json'
        }
    
    async def _send_sim_request(self, function, session, urls):
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(function(session, url))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return responses

    @staticmethod
    def parse_url(**kwargs):
        base_url = f"https://discordapp.com/api/v6/guilds/{kwargs['guild']}/messages/search?" \
            if kwargs.get('guild', None) is not None else f"https://discordapp.com/api/v6/channels/{kwargs['channel']}/messages/search?"

        _all = {
            'channel': 'channel_id',
            'content': 'content',
            'user': 'author_id',
            'nsfw': 'include_nsfw',
            'mentions': 'mentions',
            'min': 'min_id',
            'max': 'max_id',
            'has': 'has'
        }

        data = []
        for k, v in _all.items():
            r = kwargs.get(k, None)
            if isinstance(r, (list, tuple)):
                for p in r:
                    data.append((v, p))
            elif r is not None:
                data.append((v, getattr(r, 'id', r)))

        return base_url + urlencode(data)

    async def search(self, **kwargs):
        base_url = self.parse_url(**kwargs)
        _base_url = base_url

        rate = kwargs.pop('rate', None) or 5
        sleep = kwargs.pop('sleep', None) or 3

        headers = {
            'Authorization': self.auth,
            'User-Agent': self.user_agent,
            'Content-Type': 'application/json'
        }

        async with self.session.get(base_url, headers=headers) as res:
            content = await res.json()

        _content = {
            'analytics_id': content.get('analytics_id', -1),
            'total_results': int(content.get('total_results', -1)),
            'messages': []
        }

        if not bool(kwargs.get('messages', True)):
            return _content

        pages = math.ceil(int(content.get('total_results')) / 25)
        urls = [_base_url + f'&offset={i*25}' for i in range(pages)]

        async def _append(url):
            async with self.session.get(url, headers=headers) as res:
                jsn = await res.json()

            if jsn.get('retry_after', None) is not None:
                await asyncio.sleep(jsn['retry_after'] / 1000)
                async with self.session.get(url, headers=headers) as res:
                    jsn = await res.json()

            if jsn.get('messages', None) is not None:
                for message in jsn['messages']:
                    for i in range(len(message)):
                        if message[i].get('hit', None) is not None:
                            _content["messages"].append(message[i])


        for x in grouper(rate, urls):
            tasks = [asyncio.ensure_future(_append(url)) for url in x if url is not None]
            await asyncio.gather(*tasks)
            await asyncio.sleep(sleep)

        return _content
