import json
import logging
import os
import re
import requests
import ckanapi
import csv
import slugify

CONFIG_FILENAME = os.getenv('CONFIG_FILENAME', 'config.json')
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG_FILENAME)

with open(CONFIG_PATH, 'r') as config_file:
    CONFIG = json.loads(config_file.read())['config']

DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG['data_path'])
USERS_FILE = os.path.join(DATA_PATH, 'users.json')
ORGANIZATIONS_FILE = os.path.join(DATA_PATH, 'organizations.json')
RESOURCES_FILE = os.path.join(DATA_PATH, CONFIG['resources_file'])
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
    created_users = []
    with open(USERS_FILE, 'r') as users_file:
        users = json.load(users_file)['users']
        for user in users:
            try:
                new_user = ckan.action.user_create(**user)
                created_users.append(new_user)
                log.info(f"Created user {user['name']}")
                continue
            except ckanapi.errors.ValidationError as e:
                pass  # fallback to user update
            try:
                log.warning(f"User {user['name']} might already exists. Will try to update.")
                update_user = ckan.action.user_show(id=user['name'])
                user_id = update_user['id']
                ckan.action.user_update(id=user_id, **user)
                created_users.append(update_user)
                log.info(f"Updated user {user['name']}")
            except ckanapi.errors.ValidationError as e:
                log.error(f"Can't create user {user['name']}: {e.error_dict}")
    return created_users


def load_datasets(ckan, resources):
    """
    Helper method to load datasets from the RESOURCES_FILE config file
    :param ckan: ckanapi instance
    :param resources: a list of resources built from the metadata import file
    :return: None
    """

    for resource in resources:
        try:
            dataset = {
                'title': _create_title(resource['dataset']),
                'name': resource['dataset_name'],
                'type': 'oht',
                'owner_org': 'spectrum',
                'private': True,
                'notes': resource['notes'],
                'tags': resource['tags'],
                'first_year': str(resource['first_year']),
                'final_year': str(resource['final_year']),
                'country_name': resource['country_name'],
                'country_iso3_alpha': resource['country_iso3_alpha'],
                'country_iso3_num': resource['country_iso3_num'],
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


def load_resources(ckan, resources):
    """
    Helper method to load resources from the RESOURCES_FILE config file
    :param ckan: ckanapi instance
    :param resources: a list of resources built from the metadata import file
    :return: None
    """
    for resource in resources:
        if len(resource['file']) < 1:
            log.warning(f"Resource {resource_dict['name']} not created as it has no file attachment")
            continue

        resource_dict = {
            'name': resource['title'],
            'format': resource['format'],
            'url': 'upload',
            'package_id': resource['dataset_name']
        }

        file_path = os.path.join(RESOURCE_FOLDER, resource['file'])

        _upload_resource(ckan, file_path, resource_dict)


def load_data(ckan_url, ckan_api_key):
    ckan = ckanapi.RemoteCKAN(ckan_url, apikey=ckan_api_key)
    resources = _prepare_resource_data()
    created_users = load_users(ckan)
    load_organizations(ckan)

    # use user specific api keys
    for user in created_users:
        session = requests.Session()
        session.headers.update({'CKAN-Substitute-user': user['name']})
        ckan = ckanapi.RemoteCKAN(ckan_url, apikey=ckan_api_key, session=session)
        user_resources = [d for d in resources if d['user'] == user['name']]
        load_datasets(ckan, user_resources)
        load_resources(ckan, user_resources)


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
        log.error(f"Can't create resource {resource_dict['name']}: {e.error_dict}")


def _create_tags(tags_str):
    if not tags_str.strip():
        return []
    tag_names = list(map(slugify.slugify, tags_str.split(',')))
    return [{"name": tag} for tag in tag_names]


def _prepare_resource_data():
    with open(RESOURCES_FILE) as csvfile:
        metadata_reader = csv.reader(csvfile)
        start_table = False
        resources = []
        for row in metadata_reader:
            if start_table:
                resource = {
                    'title': row[2],
                    'format': row[3],
                    'file': row[4],
                    'first_year': row[5],
                    'final_year': row[6],
                    'country_name': row[9],
                    'country_iso3_alpha': row[7],
                    'country_iso3_num': row[8],
                    'notes': str(row[10]),
                    'tags': _create_tags(row[11]),
                    'dataset': row[12],
                    'dataset_name': _create_name(row[12]),
                    'user': row[13]
                }
                if len(row[11]) > 0:
                    resource['tags'] = []
                    tags = row[11].split(',')
                    for tag in tags:
                        re.sub('[^a-zA-Z0-9_/\- .]', '-', tag)
                        resource['tags'].append({'name': tag})

                resources.append(resource)
            if row[1] == 'logi_id':
                start_table = True
        return resources


if __name__ == '__main__':
    try:
        assert CONFIG['ckan_api_key'] != ''
        load_data(ckan_url=CONFIG['ckan_url'], ckan_api_key=CONFIG['ckan_api_key'])
    except AssertionError as e:
        log.error('CKAN api key missing from config.json')
