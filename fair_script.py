#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import requests
import json

from github import Github
from typing import Final, List, Optional, Dict
from decouple import config


#-------------------------------------------
#-----------------CONSTANTS-----------------
#-------------------------------------------
GIT_USER: Final[str] = config("GIT_USER")
GIT_TOKEN: Final[str] = config("GIT_TOKEN")
GIT_REPO: Final[str] = config("GIT_REPO")
LOG_FILE: Final[str] = config("LOG_FILE")
FDP_TOKEN: Final[str] = config("FDP_TOKEN")
CATALOG_ID: Final[str] = config("CATALOG_ID")
BASIC_URL: Final[str] = config("BASIC_URL")


#-------------------------------------------
#-----------------VARIABLES-----------------
#-------------------------------------------
metadataStr = " a dcat:"
catalogStr = f"dct:isPartOf <https://w3id.org/ontouml-models/catalog/{CATALOG_ID}>;\n"
licenseStr = "dct:license <https://creativecommons.org/licenses/by/4.0/>;\n"
publishDict = { "current": "PUBLISHED" }

basicHeaders = {
    'Authorization': 'Bearer ' + FDP_TOKEN
}

postHeaders = {
    'Authorization': 'Bearer ' + FDP_TOKEN, 
    'Content-Type': 'text/turtle'
}

publishHeaders = {
    'Authorization': 'Bearer ' + FDP_TOKEN, 
    'Content-Type': 'application/json'
}


#-------------------------------------------
#-------------------LOAD--------------------
#-------------------------------------------

def prepare_add(content: str) -> str:
    metadata_idx = content.find(";\n", content.find(metadataStr)) + 3
    
    # remove tail which is not needed for the request
    tail = content.find('<https://w3id.org/', metadata_idx)
    if tail > -1:
        content = content[:tail]
    
    # add license if not given
    if "dct:license" not in content:
        content = content[:metadata_idx] + licenseStr + content[metadata_idx:] 
    
    # add reference to the catalog
    if "dct:isPartOf" not in content:
        content = content[:metadata_idx] + catalogStr + content[metadata_idx:] 
        
    return content


def get_id(s: str) -> str:
    start = s[:s.find(metadataStr)].rfind('<')
    end = s[:s.find(metadataStr)].rfind('>') 
    return s[start+1:end]


def add_request(logger, data, headers=postHeaders) -> Optional[Dict]:
    """
    POST request to FDP to create a new data entry
    """
    response = requests.post(BASIC_URL, data=data, headers=headers)
    
    if response.ok:
        response = response.content.decode()
        return {
            "old_id": get_id(data), 
            "new_id": get_id(response), 
            "data": response
        }
    else:
        logger.error(response.content.decode())
        return None


def publish_request(full_id: str, headers=publishHeaders) -> bool:
    url = f"{BASIC_URL}/{full_id.split('/')[-1]}/meta/state"
    response = requests.put(url, data=json.dumps(publishDict), headers=headers)
    return response.ok


def load (logger, repository, *model_names) -> List:
    contents = []
    if model_names:
        contents = [repository.get_contents(f"models/{name}/metadata.ttl") for name in model_names]
    else:
        all_models = repository.get_contents("models")
        contents = [repository.get_contents(model.path + "/metadata.ttl") for model in all_models]

    results = []    
    for content in contents:
        logger.debug(content.path)
        response = add_request(logger, prepare_add(content.decoded_content.decode()))
        if response: # if request was successful
            publish_request(response["new_id"])
            repository.update_file(content.path, 
                                   f"update from FDP:{response['new_id']}", 
                                   response["data"], 
                                   content.sha)
            results.append({"old_id": response["old_id"], "new_id": response["new_id"], "path": content.path})
    return results

#-------------------------------------------
#------------------UPDATE-------------------
#-------------------------------------------

def get_request(logger, short_id: str, headers=basicHeaders) -> Optional[str]:
    url = f"{BASIC_URL}/{short_id}"
    response = requests.get(url, headers=headers)
    if response.ok:
        return response.content.decode()
    else:
        logger.error(f"Not possible to get information for entity {short_id}")
        logger.error(response.content.decode())
        return None

#-------------------------------------------
#------------------DELETE-------------------
#-------------------------------------------

def delete_request(logger, short_id: str, headers = basicHeaders) -> bool:
    url = f"{BASIC_URL}/{short_id}"
    response = requests.delete(url, headers=headers)
    if not response.ok:
        logger.error(f"Not possible to remove entity {short_id}")
        logger.error(response.content.decode())
    return response.ok


#-------------------------------------------
#-------------------MAIN--------------------
#-------------------------------------------
def setup_logger(name, level):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Creates a new logger only if SciorTester does not exist
    if not logger.hasHandlers():
        formatter = logging.Formatter(fmt='%(levelname)-8s %(asctime)s %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S')
        handler = logging.FileHandler(LOG_FILE, mode='w+')
        handler.setFormatter(formatter)
        screen_handler = logging.StreamHandler(stream=sys.stdout)
        screen_handler.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.addHandler(screen_handler)
    return logger


def main(logger, arguments):

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    action = parser.add_mutually_exclusive_group()
    action.add_argument('-l', '--load', action='store_true',
                        help="Load and publish the data from GitHub to the FAIR Catalog")
    action.add_argument('-u', '--update', action='store_true',
                        help="Update the data in GitHub from the FAIR Catalog")
    action.add_argument('-d', '--delete', action='store_true',
                        help="Delete the data in the FAIR Catalog")
    
    parser.add_argument('-i', '--id', action='store',
                        help="ID of the model")
                        
    args = parser.parse_args(arguments)
    logger.debug(args)

    git = Github(GIT_TOKEN)
    all_repos = list(filter(lambda repo: repo.full_name.endswith(GIT_REPO), 
                            git.get_user(GIT_USER).get_repos()))
    repo = all_repos[0] if all_repos else None
    if not repo:
        logger.error(f"Not able to find repository {GIT_REPO} for the user {GIT_USER}")
        raise SystemExit(f"Not able to find repository {GIT_REPO} for the user {GIT_USER}")

    if args.load:
        if not args.id:
            results = load(logger, repo)
        else:
            results = load(logger, repo, *[arg.strip() for arg in args.id.split(',')])
        logger.debug(results)
        
    elif args.update:
        if not args.id:
            logger.error("argument -u/--update requires an argument -i/--id")
            raise SystemExit("argument -u/--update requires an argument -i/--id")
        raise NotImplementedError("Update")

    elif args.delete:
        if not args.id:
            logger.error("argument -d/--delete requires an argument -i/--id")
            raise SystemExit("argument -d/--delete requires an argument -i/--id")
        else:
            for arg in args.id.split(','):
                delete_request(logger, arg.strip())


if __name__ == '__main__':

    logger = setup_logger("fair_pylogger", logging.DEBUG)
    sys.exit(main(logger, sys.argv[1:]))