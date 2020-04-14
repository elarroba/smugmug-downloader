import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from itertools import accumulate
from typing import Dict, List, Tuple
from zipfile import ZipFile

import requests
from requests_oauthlib import OAuth1

PROCESS_WORKERS = 3
DOWNLOAD_WORKERS = 5
MAX_DOWNLOAD_QUEUE = 3

AWAIT_SLEEP_PROCESS_ALBUM = .2
AWAIT_SLEEP_DOWNLOAD_ALBUM = 2

APPLY_ALBUM_LIMIT = True
MAKE_DIRS = False
RESET_PROGRESS = False
EXECUTE_DOWNLOADS = True
UNZIP_ALBUMS = False

DEBUG = True


async def load_settings() -> dict:
    try:
        with open('./smgbkp/config.json', 'r') as config_json:
            config = json.load(config_json)

            params = {
                '_accept': 'application/json',
                'APIKey': config['app_key'],
            }

            auth_data = OAuth1(config['app_key'],
                               config['app_key_secret'],
                               config['access_key'],
                               config['access_key_secret'])

            return {
                'config': config,
                'params': params,
                'auth': auth_data
            }

    except IOError:
        print('Could not open config.json. Please ensure it exists')
        sys.exit(1)


class SmugMugSession:

    def __init__(self, config: dict):
        self.config = config
        self.USER = config['config']['user']
        self.OAuth1Data = config['auth']
        self.PARAMS: dict = config['params']

        self.STORE_BASE_DIR = './store'
        if config['config']['output_dir']:
            self.STORE_BASE_DIR = config['config']['output_dir']
        self.STORE_BASE_DIR = os.path.abspath(self.STORE_BASE_DIR)

        if not os.path.exists(self.STORE_BASE_DIR):
            os.makedirs(self.STORE_BASE_DIR, exist_ok=True)

        self.BASE_URL = 'https://api.smugmug.com'
        self.ALBUM_COUNT: int or None = None
        self.ALBUM_LIST: Tuple[Dict] or None = None
        self.ALBUM_DATA_JSON: str = 'album_data.json'
        self.ALBUM_LIST_STEP: int = 100
        self.ALBUM_LIMIT: int = self.config['config']['album_limit']

        self.ALBUMS_ZIP_STORE = 'albums_zip'
        self.ALBUMS_ZIP_PATH: str = os.path.join(self.STORE_BASE_DIR, self.ALBUMS_ZIP_STORE)

        self.ALBUMS_MEDIA_STORE = 'albums_media'
        self.ALBUMS_MEDIA_PATH: str = os.path.join(self.STORE_BASE_DIR, self.ALBUMS_MEDIA_STORE)

        self.IMAGE_COUNT: int or None = None
        self.IMAGE_LIMIT: int = self.config['config']['image_limit']

        self.logger = logging.getLogger()

    async def get_album_count(self):
        if not self.ALBUM_COUNT:
            response = await self.get_album_range(start=0, step=2)
            self.ALBUM_COUNT = response['Response']['Pages']['Total']
        return self.ALBUM_COUNT

    async def get_album_range(self, start: int = None, step: int = 100):

        url = f'{self.BASE_URL}/api/v2/user/{self.USER}!albums'
        params = self.PARAMS
        if start is not None and step is not None:
            params['start'] = start
            params['count'] = step
        album_response = requests.get(url, params=params, auth=self.OAuth1Data)
        if album_response.ok:
            return album_response.json()
        else:
            self.logger.error(
                f'Could not download album info. Got status code {album_response.status_code}')
            sys.exit(1)

    async def get_album_list(self,
                             update_data: bool = False,
                             reset_progress: bool = False) -> Tuple[Dict]:

        if not self.ALBUM_LIST:

            f_path = os.path.join(self.STORE_BASE_DIR, self.ALBUM_DATA_JSON)
            if not os.path.exists(f_path) or update_data:

                album_count = await self.get_album_count()
                albums = list()
                for r in range(0, album_count, self.ALBUM_LIST_STEP):
                    r_albums = await self.get_album_range(start=r, step=r + self.ALBUM_LIST_STEP)
                    r_albums = r_albums['Response']['Album']
                    albums += r_albums
                    print(f'{datetime.now()} - {r}-{r + self.ALBUM_LIST_STEP} albums fetched...')
                    await asyncio.sleep(.5)
                for al in albums:
                    al['downloaded'] = False
                    al['processed'] = False
                    al['unzipped'] = False

                albums.sort(reverse=True, key=lambda alb: datetime.fromisoformat(alb['Date']))
                with open(f_path, 'w') as output:
                    json.dump({
                        'album_list': albums
                    }, output)
                    output.close()
                self.IMAGE_COUNT = sum(a['ImageCount'] for a in albums)
                self.ALBUM_LIST = tuple(albums)

            else:
                with open(f_path, 'r') as input_json:
                    albums = json.load(input_json)['album_list']
                    input_json.close()
                if reset_progress:
                    for al in albums:
                        al['processed'] = False
                        al['downloaded'] = False
                        al['unzipped'] = False

                albums.sort(reverse=True, key=lambda alb: datetime.fromisoformat(alb['Date']))
                self.IMAGE_COUNT = sum(a['ImageCount'] for a in albums)
                self.ALBUM_LIST = tuple(albums)
        return self.ALBUM_LIST

    async def get_albums_to_process(self, apply_limits: bool = False):
        album_list = await self.get_album_list()
        if apply_limits:
            al_count = 0
            accum = accumulate(a['ImageCount'] for a in self.ALBUM_LIST)
            while next(accum) < self.IMAGE_LIMIT:
                al_count += 1
            limit = min(al_count, self.ALBUM_LIMIT)
            return album_list[:limit]
        return album_list

    async def get_album_dir(self, album: dict, media_dir: bool = False, alt_dir: str = None) -> str:
        if media_dir:
            alt_dir = self.ALBUMS_MEDIA_PATH
        elif alt_dir:
            alt_dir = os.path.join(self.STORE_BASE_DIR, alt_dir)
        else:
            alt_dir = self.ALBUMS_ZIP_PATH
        return os.path.join(alt_dir, album['NiceName'])

    async def get_zip_file_list(self, album_list: List[Dict] = None) -> list:
        if not album_list:
            album_list = await self.get_album_list()

        file_list = list()
        for album in album_list:
            album_dir = await self.get_album_dir(album=album)
            dir_list = os.listdir(album_dir)
            file_list += [os.path.join(album_dir, f) for f in dir_list if f.split('.')[-1] == 'zip']
        return list(set(file_list))

    async def get_media_files_list(self, album_list: List[Dict] = None) -> list:
        if not album_list:
            album_list = await self.get_album_list()

        file_list = list()
        for album in album_list:
            album_dir = await self.get_album_dir(album=album,
                                                 media_dir=True)
            dir_list = os.listdir(album_dir)
            file_list += [os.path.join(album_dir, f) for f in dir_list if f.split('.')[-1] != 'zip']
        return list(set(file_list))

    async def get_downloaded_count(self):
        return sum(a['downloaded'] for a in self.ALBUM_LIST if a['downloaded'])

    async def get_allowed_downloads(self):
        downloaded = await self.get_downloaded_count()
        allowed_downloads = self.ALBUM_LIMIT - downloaded
        if allowed_downloads > 0:
            return allowed_downloads
        return 0

    async def get_download_uri(self, album: dict) -> str:
        return self.BASE_URL + album['Uris']['AlbumDownload']['Uri']

    async def make_dirs(self,
                        album_list: Tuple[Dict] = None,
                        alt_dir: str = None):

        if not album_list:
            album_list = await self.get_album_list()

        for album in album_list:
            dirs_to_create = list()
            dirs_to_create.append(await self.get_album_dir(album=album, alt_dir=alt_dir))
            dirs_to_create.append(await self.get_album_dir(album=album, media_dir=True, alt_dir=alt_dir))

            for p in dirs_to_create:
                if not os.path.exists(p):
                    os.makedirs(
                        p,
                        exist_ok=True
                    )

    async def unzip_albums(self):

        zip_files = [f for f in await self.get_zip_file_list()]
        total_files = len(zip_files)
        for i, zip_file in enumerate(zip_files):
            with ZipFile(zip_file, 'r') as zf:
                zf.extractall(path=os.path.dirname(zip_file))
                zf.close()
            print(f'Zip File {zip_file} extracted. {i + 1}/{total_files}...')

    def save_json(self):
        f_path = os.path.join(self.STORE_BASE_DIR,
                              self.ALBUM_DATA_JSON)

        with open(f_path, 'w') as output_json:
            json.dump({
                'album_list': self.ALBUM_LIST
            }, output_json)
            output_json.close()


async def download_album_zip(album: dict, uri: str, file_path: str, auth, worker_id: int):
    response = requests.get(uri,
                            allow_redirects=True,
                            stream=True,
                            auth=auth,
                            timeout=20.0)
    content_length = int(response.headers['content-length'])
    if os.path.exists(file_path):
        if os.path.getsize(file_path) == content_length:
            return None
    if response.ok:
        print(f'{datetime.now()} - Worker {worker_id}: Saving album {album["Title"]}...')
        with open(file_path, 'wb') as fd:
            saved = 0
            cycle = 0
            for chunk in response.iter_content(1024):
                fd.write(chunk)
                saved += 1024
                cycle += 1
                if cycle % 5000 == 0:
                    print(f'{datetime.now()} - Worker {worker_id}: {saved / content_length}% bytes saved...')
            fd.close()


async def download_albums_worker(worker_id: int,
                                 smugmug_session: SmugMugSession,
                                 download_queue: asyncio.Queue):
    try:
        while True:
            album = await download_queue.get()
            await asyncio.sleep(AWAIT_SLEEP_DOWNLOAD_ALBUM)
            if not album['downloaded']:
                title = album['Title']
                download_uri = await smugmug_session.get_download_uri(album)
                print(f'{datetime.now()} - Download Worker {worker_id}: DOWNLOADING album {title}... {download_uri}')
                download_response = requests.get(download_uri,
                                                 params=smugmug_session.PARAMS,
                                                 auth=smugmug_session.OAuth1Data)
                response_json = download_response.json()

                if download_response.ok and response_json['Message'] == 'Ok':

                    while 'Download' not in response_json['Response']:
                        print(f'{datetime.now()} - Download not ready {album["Title"]}')
                        await asyncio.sleep(5)
                        download_response = requests.get(url=download_uri,
                                                         params=smugmug_session.PARAMS,
                                                         auth=smugmug_session.OAuth1Data)
                        if download_response.ok:
                            response_json = download_response.json()
                    await asyncio.sleep(AWAIT_SLEEP_DOWNLOAD_ALBUM)
                    print(
                        f'{datetime.now()} - Its time to download {album["Title"]}!!!....')

                    if not album.get('download_list'):
                        album['download_list'] = list()

                    for download in response_json['Response']['Download']:
                        if download['Status'] == 'Error':
                            print(f'{datetime.now()} - WORKER: {worker_id} - '
                                  f'No WebUri {album["Title"]}, Download Failed')
                            album['download_weburi_error'] = True
                            album['download_weburi_error_dt'] = datetime.now().isoformat()
                        else:
                            uri = download['WebUri']
                            print(f'{datetime.now()} - WORKER: {worker_id} - '
                                  f'Iterating through download list {album["Title"]}, {uri}')
                            file_name = download['FileName']
                            album_dir = await smugmug_session.get_album_dir(album=album)
                            file_path = os.path.join(album_dir, file_name)
                            album['download_list'].append({
                                'uri': uri,
                                'file_name': file_name,
                                'file_path': file_path
                            })

                            await download_album_zip(album=album,
                                                     uri=uri,
                                                     file_path=file_path,
                                                     auth=smugmug_session.OAuth1Data,
                                                     worker_id=worker_id)

                            await asyncio.sleep(3)
                        album['downloaded'] = True
                        album['downloaded_dt'] = datetime.now().isoformat()
                else:
                    print(
                        f'{datetime.now()} - WORKER: {worker_id} - Response error downloading {album["Title"]}'
                    )
                    await download_queue.put(album)
            download_queue.task_done()
            await asyncio.sleep(AWAIT_SLEEP_DOWNLOAD_ALBUM)
    except asyncio.CancelledError:
        print(f'{datetime.now()} - Shutting down download WORKER: {worker_id}')


async def process_albums_worker(worker_id: int,
                                smugmug_session: SmugMugSession,
                                process_queue: asyncio.Queue,
                                download_queue: asyncio.Queue
                                ):
    try:
        while True:
            album = await process_queue.get()
            await asyncio.sleep(AWAIT_SLEEP_PROCESS_ALBUM)
            if not album['processed']:
                title = album['Title']
                process_uri = await smugmug_session.get_download_uri(album)
                print(
                    f'{datetime.now()} - Process Worker {worker_id}: Processing album {title}... - TOGO: {process_queue.qsize()} - URI: {process_uri}'
                )
                process_response = requests.post(url=process_uri,
                                                 params=smugmug_session.PARAMS,
                                                 auth=smugmug_session.OAuth1Data)
                process_response_json = process_response.json()

                if process_response.ok and process_response_json['Message'] == 'Ok':
                    album['processed_dt'] = datetime.now().isoformat()
                    album['processed'] = True
                    print(f'{datetime.now()} - Awaiting queue to download album {album["Title"]}...')
                    await asyncio.sleep(AWAIT_SLEEP_PROCESS_ALBUM)
                    await download_queue.put(album)
                elif process_response.status_code == 403:
                    message = process_response_json['Message']
                    album['processed'] = True
                    album['processed_error'] = process_response_json['Message']
                    album['processed_dt'] = datetime.now().isoformat()

                    album['downloaded'] = True
                    album['download_skip'] = True
                    album['download_skip_dt'] = datetime.now().isoformat()
                    print(
                        f'{datetime.now()} - Error response {process_response.status_code} '
                        f'skipping album {album["Title"]} - {message}...'
                    )
                else:
                    print(f'{datetime.now()} - Error response processing album {album["Title"]}...')
                    process_queue.put_nowait(album)
            elif album['processed'] and not album['downloaded']:
                print(f'{datetime.now()} - Album {album["Title"]} already processed, putting in download queue...')
                await download_queue.put(album)
            process_queue.task_done()
            await asyncio.sleep(AWAIT_SLEEP_PROCESS_ALBUM)
    except asyncio.CancelledError:
        print(f'{datetime.now()} - Shutting down process WORKER {worker_id}')


async def main():
    config = await load_settings()
    smg_session = SmugMugSession(config=config)
    albums_to_process = await smg_session.get_albums_to_process(apply_limits=APPLY_ALBUM_LIMIT)

    try:

        if MAKE_DIRS:
            await smg_session.make_dirs(albums_to_process)

        total_downloaded = await smg_session.get_downloaded_count()
        if all([EXECUTE_DOWNLOADS,
                total_downloaded < smg_session.ALBUM_LIMIT]):

            process_queue = asyncio.Queue()
            download_queue = asyncio.Queue(maxsize=MAX_DOWNLOAD_QUEUE)

            for album in albums_to_process:
                if not all([
                    album['processed'],
                    album['downloaded']
                ]):
                    process_queue.put_nowait(album)

            tasks = list()
            for i in range(PROCESS_WORKERS):
                task = asyncio.create_task(process_albums_worker(worker_id=i,
                                                                 smugmug_session=smg_session,
                                                                 process_queue=process_queue,
                                                                 download_queue=download_queue))
                tasks.append(task)

            for i in range(DOWNLOAD_WORKERS):
                task = asyncio.create_task(download_albums_worker(worker_id=i,
                                                                  smugmug_session=smg_session,
                                                                  download_queue=download_queue))
                tasks.append(task)

            await process_queue.join()
            print(f'{datetime.now()} - Process Queue Joined...')
            await download_queue.join()
            print(f'{datetime.now()} - Download Queue Joined...')

            for task in tasks:
                task.cancel()
            results = await asyncio.gather(*tasks, return_exceptions=True)
        elif total_downloaded >= smg_session.ALBUM_LIMIT:
            print(
                f'{datetime.now()} - No more downloads allowed. Limit {smg_session.ALBUM_LIMIT} albums '
                f'or {smg_session.IMAGE_LIMIT} images...'
            )

        if UNZIP_ALBUMS:
            await smg_session.unzip_albums()

        # await smg_session.make_dirs(alt_dir='albums_zip')
        # zip_files = await smg_session.get_zip_file_list()
        # from_to_map = dict()
        # store_dir = pathlib.Path(smg_session.STORE_DIR).joinpath('albums_zip')
        # for from_zip in zip_files:
        #     from_zip_path = pathlib.Path(from_zip)
        #     to_zip = pathlib.Path(*from_zip_path.parts[-2:])
        #     to_zip = store_dir.joinpath(to_zip)
        #     from_to_map[from_zip] = to_zip
        #
        # for o, d in from_to_map.items():
        #     shutil.move(o, d.parent)
        #     print(f'File {o} moved to {d}')

    finally:
        smg_session.save_json()


asyncio.run(main())
