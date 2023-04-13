#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import requests
import json
import re
import xmltodict

from github import Github, InputGitTreeElement
from typing import Final, List, Optional, Dict
from decouple import config


#-------------------------------------------
#-----------------CONSTANTS-----------------
#-------------------------------------------
GIT_USER: Final[str] = config("GIT_USER")
GIT_TOKEN: Final[str] = config("GIT_TOKEN")
GIT_REPO: Final[str] = config("GIT_REPO")
GIT_BRANCH: Final[str] = config("GIT_BRANCH")
LOG_FILE: Final[str] = config("LOG_FILE")
FDP_TOKEN: Final[str] = config("FDP_TOKEN")
CATALOG_ID: Final[str] = config("CATALOG_ID")
BASIC_URL: Final[str] = config("BASIC_URL")
FDP_PREFIX: Final[str] = config("FDP_PREFIX")


#-------------------------------------------
#-----------------VARIABLES-----------------
#-------------------------------------------
metadataStr = " a dcat:"
catalogStr = f"    dct:isPartOf <https://w3id.org/ontouml-models/catalog/{CATALOG_ID}>;\n"
licenseStr = "    dct:license <https://creativecommons.org/licenses/by/4.0/>;\n"
modelStr = f"    dct:isPartOf <>;\n"
resourceStr = "dcat:Resource"
distributionStr = "dcat:distribution"
issuedStr = "dct:issued"
publishDict = { "current": "PUBLISHED" }
fdpAddress = FDP_PREFIX[FDP_PREFIX.find('<')+1:FDP_PREFIX.find('>')]
fdpShort = FDP_PREFIX.split(' ')[1]


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
#-----------------HELPERS-------------------
#-------------------------------------------

def is_additional_ttl(s: str) -> bool:
    """Check if the file path is metadata_xxx.ttl"""
    # len("metadata.ttl") == 12
    return s.endswith(".ttl") and (len(s) > 12)


def get_full_id(s: str) -> str:
    """Returns a main id from the ttl document"""
    start = s[:s.find(metadataStr)].rfind('<')
    end = s[:s.find(metadataStr)].rfind('>') 
    return s[start+1:end].rstrip('/')


def get_issued(s: str) -> str:
    """Returns an issued date"""
    for line in s.split('\n'):
        if issuedStr in line:
            return line


def get_license(s: str) -> str:
    """Returns a license string"""
    for line in s.split('\n'):
        if "dct:license" in line:
            return line


def is_new_id(full_id: str) -> bool:
    """Check if the ttl already has a new id"""
    id_pattern = re.compile('\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}')
    _id = full_id.split('/')[-1]
    return bool(id_pattern.match(_id))


def get_distributions(content: str) -> str:
    """Get the list of the distributions"""
    distributions = [""]  # at least one element exists
    for line in content.split('\n'):
        if distributionStr in line:
            distributions = list(filter(None, re.split(' <|>.|>, <', line[line.find(distributionStr):])))
    return ">, <".join(distributions[1:])


def prepare_model_add(content: str) -> str:
    """Updates the original ttl so it can be used in the request"""
    metadata_idx = content.find(";\n", content.find(metadataStr)) + 2
    
    # remove the tail with distributions which is not needed for the request
    tail = content.find('<https://w3id.org/', content.find("storageUrl"))
    if tail > -1:
        content = content[:tail]
    
    # add license if not given
    if "dct:license" not in content:
        content = content[:metadata_idx] + licenseStr + content[metadata_idx:] 
    
    # add reference to the catalog
    if "dct:isPartOf" not in content:
        content = content[:metadata_idx] + catalogStr + content[metadata_idx:]       
        
    if resourceStr not in content:
        content = content[:metadata_idx-2] + ", " + resourceStr + content[metadata_idx-2:]
        
    return content


def prepare_distr_add(content: str, model_id: str, issued: str, license: str) -> str: 
    """Updates the original distribution ttl so it can be used in the request"""
    metadata_idx = content.find(";\n", content.find(metadataStr)) + 2
    
    # add license if not given
    if "dct:license" not in content:
        content = content[:metadata_idx] + license + content[metadata_idx:] 
    
    # add issued if not given
    if issuedStr not in content:
        content = content[:metadata_idx] + issued + "\n" + content[metadata_idx:] 
    
    # add reference to the model
    if "dct:isPartOf" not in content:
        content = content[:metadata_idx] + modelStr[:-3] + model_id + modelStr[-3:] + content[metadata_idx:]    
            
    return content


def add_distributions(data: str, _id: str, distributions: str) -> str:
    """Forms new distributions from the dictionary"""
    if distributionStr in data:
        return data
    else:
        return data + f"    <{_id}> {distributionStr} <{distributions}>."
    

def add_metadata(old_data: str, old_id: str, new_data: str) -> str:
    """Adds info about the metadata and also updates the id"""
    data = old_data.replace(old_id, get_full_id(new_data))
    
    if FDP_PREFIX not in data:
        data = FDP_PREFIX + '\n' + data
        
    if data.rstrip().endswith("."):
        data = data.rstrip()[:-1] + ";\n"
    
    for line in new_data.split('\n'):
        if fdpAddress in line:
            if 'metadataIssued' in line:
                data += f"""    {fdpShort}metadataIssued{line[line.find(">")+1:]}\n"""
            if 'metadataModified' in line:
                data += f"""    {fdpShort}metadataModified{line[line.find(">")+1:]}\n"""

    return data

#-------------------------------------------
#----------------REQUESTS-------------------
#-------------------------------------------
    
def add_request(logger, data, headers = postHeaders, url = BASIC_URL) -> Optional[str]:
    """Sends a request to add the model to FDP and returns a new id"""
    response = requests.post(url, data=data.encode('utf-8') , headers=headers)
    if response.ok:
        return response.content.decode()
    else:
        logger.error(response.content.decode())
        return None
    
    
def publish_request(full_id: str, headers = publishHeaders, url = BASIC_URL) -> bool:
    """Publish the data to the FDP"""
    url = f"{url}/{full_id.split('/')[-1]}/meta/state"
    response = requests.put(url, data=json.dumps(publishDict), headers=headers)
    return response.ok


def delete_request(logger, full_id: str, headers = basicHeaders, url = BASIC_URL) -> bool:
    url = f"{url}/{full_id.split('/')[-1]}"
    logger.debug("DELETE: " + url)
    response = requests.delete(url, headers=headers)
    return response.ok


def get_request(logger, full_id: str, headers = basicHeaders, url = BASIC_URL) -> bool:
    """Check if the data is already in the FDP"""
    url = f"{url}/{full_id.split('/')[-1]}"
    response = requests.get(url, headers=headers)
    if response.ok:
        return full_id == get_full_id(response.content.decode())
    else:
        logger.error(response.content.decode())
        return False

#-------------------------------------------
#-------------------LOAD--------------------
#-------------------------------------------

def make_commit(logger, repository, branch: str, elements: List, message: str):
    try:
        branch_sha = repository.get_branch(branch).commit.sha
        base_tree = repository.get_git_tree(sha=branch_sha)
        tree = repository.create_git_tree(elements, base_tree)
        parent = repository.get_git_commit(sha=branch_sha)
        commit = repository.create_git_commit(message, tree, [parent])
        branch_refs = repository.get_git_ref(f"heads/{branch}")
        branch_refs.edit(sha=commit.sha)
    except Exception as err:
        logger.error(err)


    
def load (logger, repository, publish: bool, branch: str, *model_names) -> List:
    results = []
    contents = []
    elements = []
    
    if model_names:
        contents = [repository.get_contents(f"models/{name}/metadata.ttl", ref=branch) for name in model_names]
    else:
        all_models = repository.get_contents("models")
        contents = [repository.get_contents(model.path + "/metadata.ttl", ref=branch) for model in all_models]

    for content in contents:
        logger.debug(content.path)
        model_ttl = content.decoded_content.decode()
        model_id = get_full_id(model_ttl)
        distributions = get_distributions(model_ttl)
        
        if (not is_new_id(model_id)) or (not get_request(logger, model_id, url=BASIC_URL+"model")): 
            old_model_id = model_id
            model_ttl = prepare_model_add(model_ttl)
            model_issued = get_issued(model_ttl)
            model_license = get_license(model_ttl)
            
            try:
                model_new_data = add_request(logger, model_ttl, url=BASIC_URL+"model")  
                if model_new_data:
                    model_id = get_full_id(model_new_data)
                    if publish:
                        publish_request(model_id, url=BASIC_URL+"model") 
                    model_ttl = add_metadata(model_ttl, old_model_id, model_new_data)
            except Exception as err:
                logger.error(err)

        if model_id:
            # get all the ttl files from the corresponding directory
            all_ttls = [ttl for ttl in repository.get_contents(os.path.dirname(content.path), ref=branch) 
                            if is_additional_ttl(os.path.basename(ttl.path))]
            for ttl in all_ttls:
                try:
                    ttl_content = ttl.decoded_content.decode()
                    ttl_id = get_full_id(ttl_content)

                    if (not is_new_id(ttl_id)) or (not get_request(logger, ttl_id, url=BASIC_URL+"distribution")): 
                        old_ttl_id = ttl_id
                        ttl_data = prepare_distr_add(ttl_content, model_id, model_issued, model_license)
                        ttl_new_data = add_request(logger, ttl_data, url=BASIC_URL+"distribution")
                        if ttl_new_data:
                            ttl_id = get_full_id(ttl_new_data)
                            distributions = distributions.replace(old_ttl_id, ttl_id)
                            if publish:
                                publish_request(ttl_id, url=BASIC_URL+"distribution")

                            ttl_data = add_metadata(ttl_data, old_ttl_id, ttl_new_data)
                            repository.update_file(ttl.path, f"FDP: {model_id}", ttl_data, ttl.sha,  branch=branch)
                            # blob = repository.create_git_blob(ttl_data, "utf-8")
                            # elements.append(InputGitTreeElement(path=ttl.path, mode='100644', type='blob', sha=blob.sha))

                except Exception as err:
                    logger.error(err)

            model_ttl = add_distributions(model_ttl, model_id, distributions)
            repository.update_file(content.path, f"FDP: {model_id}", model_ttl, content.sha, branch=branch)
            # blob = repository.create_git_blob(model_ttl, "utf-8")
            # elements.append(InputGitTreeElement(path=content.path, mode='100644', type='blob', sha=blob.sha))

            # make_commit(logger, repository, branch, elements, f"FDP: {model_id}")

        results.append(content.path)
    return results

#-------------------------------------------
#------------------DELETE-------------------
#-------------------------------------------

def get_all_data(logger, isDistr: bool, headers = basicHeaders, url = BASIC_URL+"blazegraph/sparql"):
    substr = "rdf:type dcat:Distribution" if isDistr else \
            f"dct:isPartOf <https://w3id.org/ontouml-models/catalog/{CATALOG_ID}>"
    query = """
    PREFIX dct: <http://purl.org/dc/terms/> 
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    SELECT ?s
    WHERE {
        ?s """ + substr + """ .
    }
    """
    response = requests.get(url, headers=headers, params={"query": query})
    
    results = set()
    if response.ok:
        full_dict = xmltodict.parse(response.content.decode())
        try:
            for binding in full_dict["sparql"]["results"]["result"]:
                results.add(binding["binding"]["uri"])
        except:
            logger.debug("Empty result set") 
    else:
        logger.error(response.content.decode())
    return list(results)


def delete_all(logger):
    resources = get_all_data(logger, True)
    models = get_all_data(logger, False)
    for resource in resources:
        delete_request(logger, resource, url = BASIC_URL+"distribution")
    for model in models:
        delete_request(logger, model, url = BASIC_URL+"model")

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
            results = load(logger, repo, True, GIT_BRANCH)
        else:
            results = load(logger, repo, True, GIT_BRANCH, *[arg.strip() for arg in args.id.split(',')])
        logger.debug(results)
        
    elif args.update:
        if not args.id:
            logger.error("argument -u/--update requires an argument -i/--id")
            raise SystemExit("argument -u/--update requires an argument -i/--id")
        raise NotImplementedError("Update")

    elif args.delete:
        delete_all(logger)


if __name__ == '__main__':

    logger = setup_logger("fair_pylogger", logging.DEBUG)
    sys.exit(main(logger, sys.argv[1:]))
