import json
import logging
import os
import re

import ckanapi

import csv
import zipfile
import rarfile
import shutil

import slugify

CONFIG_FILENAME = os.getenv('CONFIG_FILENAME', 'config.json')
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG_FILENAME)

with open(CONFIG_PATH, 'r') as config_file:
    CONFIG = json.loads(config_file.read())['config']

DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG['data_path'])
USERS_FILE = os.path.join(DATA_PATH, 'users.json')
ORGANIZATIONS_FILE = os.path.join(DATA_PATH, 'organizations.json')
DOCUMENTS_FILE = os.path.join(DATA_PATH, CONFIG['documents_file'])
GROUPS_FILE = os.path.join(DATA_PATH, CONFIG['groups_file'])
RESOURCE_FOLDER = os.path.join(DATA_PATH, CONFIG['resource_folder'])

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def load_organizations(ckan):
    """
    Helper method to load organizations from the ORGANIZATIONS_FILE config file
    :param ckan: ckanapi instance
    :return: a dictionary map of created organization names to their ids
    """
    organization_ids_dict = {}
    with open(ORGANIZATIONS_FILE, 'r') as organizations_file:
        organizations = json.load(organizations_file)['organizations']
        for organization in organizations:
            org_name = organization['name']
            try:
                org = ckan.action.organization_create(**organization)
                log.info(f"Created organization {org_name}")
                organization_ids_dict[org_name] = org["id"]
                continue
            except ckanapi.errors.ValidationError as e:
                pass  # fallback to organization update
            try:
                log.warning(f"Organization {org_name} might already exists. Will try to update.")
                org_id = ckan.action.organization_show(id=org_name)['id']
                ckan.action.organization_update(id=org_id, **organization)
                organization_ids_dict[org_name] = org_id
                log.info(f"Updated organization {org_name}")
            except ckanapi.errors.ValidationError as e:
                log.error(f"Can't create organization {org_name}: {e.error_dict}")
    return organization_ids_dict


def load_users(ckan):
    """
    Helper method to load users from USERS_FILE config json file
    :param ckan: ckanapi instance
    :return: None
    """
    with open(USERS_FILE, 'r') as users_file:
        users = json.load(users_file)['users']
        created_users = []
        for user in users:
            try:
                new_user = ckan.action.user_create(**user)
                created_users = new_user
                api_key = ckan.action.api_token_create(id=new_user['id'], name='demo_data_upload')
                new_user['api_key'] = api_key
                log.info(f"Created user {user['name']}")
                continue
            except ckanapi.errors.ValidationError as e:
                pass  # fallback to user update
            try:
                log.warning(f"User {user['name']} might already exists. Will try to update.")
                update_user = ckan.action.user_show(id=user['name'])['id']
                user_id = update_user['id']
                ckan.action.user_update(id=user_id, **user)
                api_key = ckan.action.api_token_create(id=new_user['id'], name='demo_data_upload')
                update_user['api_key'] = api_key
                log.info(f"Updated user {user['name']}")
            except ckanapi.errors.ValidationError as e:
                log.error(f"Can't create user {user['name']}: {e.error_dict}")
    return created_users


def load_datasets(ckan, documents):
    """
    Helper method to load datasets from the DOCUMENTS_FILE config file
    :param ckan: ckanapi instance
    :param documents: a list of documents built from the metadata import file
    :return: None
    """

    for document in documents:
        try:
            dataset = {
                'title': _create_title(document['dataset']),
                'name': document['dataset_name'],
                'type': 'oht',
                'owner_org': 'spectrum',
                'notes': document['notes'],
                'tags': document['tags'],
                'start_year': str(document['start_year']),
                'end_year': str(document['end_year']),
                'country_code': document['country_code']
            }

            ckan.action.package_create(**dataset)
            log.info(f"Created dataset {dataset['name']}")
            continue
        except ckanapi.errors.ValidationError as e:
            pass  # fallback to dataset update
        try:
            log.warning(f"Dataset {dataset['name']} might already exists. Will try to update.")
            id = ckan.action.package_show(id=dataset['name'])['id']
            ckan.action.package_update(id=id, **dataset)
            log.info(f"Updated dataset {dataset['name']}")
        except ckanapi.errors.ValidationError as e:
            log.error(f"Can't create dataset {dataset['name']}: {e.error_dict}")


def load_resources(ckan, documents):
    """
    Helper method to load resources from the DOCUMENTS_FILE config file
    :param ckan: ckanapi instance
    :param documents: a list of documents built from the metadata import file
    :return: None
    """
    for document in documents:
        if len(document['file']) < 1:
            log.warning(f"Resource {resource_dict['name']} not created as it has no file attachment")
            continue

        resource_dict = {
            'title': document['title'],
            'name': document['name'],
            'url': 'upload',
            'package_id': document['dataset_name']
        }

        file_path = os.path.join(RESOURCE_FOLDER, document['file'])

        _upload_resource(ckan, file_path, resource_dict)


def load_data(ckan_url, ckan_api_key):
    ckan = ckanapi.RemoteCKAN(ckan_url, apikey=ckan_api_key)

    documents = _load_documents()

    created_users = load_users(ckan)
    load_organizations(ckan)

    # use user specific api keys
    for user in created_users:
        ckan = ckanapi.RemoteCKAN(ckan_url, apikey=user['api_key'])
        user_documents = [d for d in documents if d['user'] == user]
        load_datasets(ckan, user_documents)
        load_resources(ckan, user_documents)


def _create_name(title):
    name = re.sub('[^a-zA-Z0-9_ /]', '', title)
    name = re.sub('[_ /]', '-', name)
    name = name.lower()
    return name


def _create_title(title):
    title = re.sub('[_]', ' ', title)
    return title


def _upload_resource(ckan, file_path, resource_dict):
    try:
        with open(file_path, 'rb') as res_file:
            ckan.call_action(
                'resource_create',
                resource_dict,
                files={'upload': res_file}
            )
        log.info(f"Created resource {resource_dict['name']}")
        return
    except ckanapi.errors.ValidationError as e:
        pass  # fallback to resource update
    try:
        log.warning(f"Resource {resource_dict['name']} might already exists. Will try to update.")
        id = ckan.action.resource_show(id=resource_dict['name'])['id']
        ckan.action.resource_update(id=id, **resource_dict)
        log.info(f"Updated resource {resource_dict['name']}")
    except ckanapi.errors.ValidationError as e:
        log.error(f"Can't create resource {resource_dict['name']}: {e.error_dict}")


def _unpack_zip(ckan, file_path, resource_dict):
    extract_folder = os.path.join(DATA_PATH, 'tmp')
    if not os.path.exists(extract_folder):
        os.makedirs(extract_folder)

    try:
        with zipfile.ZipFile(file_path) as zf:
            zf.extractall(extract_folder)
            files = zf.namelist()
            for filename in files:
                title = os.path.splitext(filename)[0]
                resource_dict['title'] = title
                resource_dict['name'] = _create_name(title)
                resource_dict['format'] = re.sub('[/.]', '', os.path.splitext(filename)[1]).upper()
                extracted_file_path = os.path.join(extract_folder, filename)
                _upload_resource(ckan, extracted_file_path, resource_dict)
    except Exception as e:
        log.error(str(e))
    finally:
        shutil.rmtree(extract_folder)


def _unpack_rar(ckan, file_path, resource_dict):
    extract_folder = os.path.join(DATA_PATH, 'tmp')
    if not os.path.exists(extract_folder):
        os.makedirs(extract_folder)

    try:
        with rarfile.RarFile(file_path) as rf:
            rf.extractall(extract_folder)
            files = rf.namelist()
            for filename in files:
                title = os.path.splitext(filename)[0]
                resource_dict['title'] = title
                resource_dict['name'] = _create_name(title)
                resource_dict['format'] = re.sub('[/.]', '', os.path.splitext(filename)[1]).upper()
                extracted_file_path = os.path.join(extract_folder, filename)
                if not os.path.isdir(extracted_file_path):
                    _upload_resource(ckan, extracted_file_path, resource_dict)
    except Exception as e:
        log.error(str(e))
    finally:
        shutil.rmtree(extract_folder)


def _create_tags(tags_str):
    if not tags_str.strip():
        return []
    tag_names = list(map(slugify.slugify, tags_str.split(',')))
    return [{"name": tag} for tag in tag_names]


def _load_documents():
    with open(DOCUMENTS_FILE) as csvfile:
        metadata_reader = csv.reader(csvfile)
        start_table = False
        documents = []
        for row in metadata_reader:
            if start_table:
                document = {
                    'title': row[2],
                    'name': _create_name(row[2]),
                    'file': row[4],
                    'start_year': row[5],
                    'end_year': row[6],
                    'country_code': row[7],
                    'notes': str(row[8]),
                    'tags': _create_tags(row[9]),
                    'dataset': row[10],
                    'dataset_name': row[11],
                    'user': row[12]
                }
                if len(row[9]) > 0:
                    document['tags'] = []
                    tags = row[9].split(',')
                    for tag in tags:
                        re.sub('[^a-zA-Z0-9_/\- .]', '-', tag)
                        document['tags'].append({'name': tag})

                documents.append(document)
            if row[1] == 'logi_id':
                start_table = True
        return documents


if __name__ == '__main__':
    try:
        assert CONFIG['ckan_api_key'] != ''
        load_data(ckan_url=CONFIG['ckan_url'], ckan_api_key=CONFIG['ckan_api_key'])
    except AssertionError as e:
        log.error('CKAN api key missing from config.json')

