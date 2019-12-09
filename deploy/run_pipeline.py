#!/usr/bin/env python

#########################################
# Azkaban Flow Runner
#
#   This script should kick off a particular flow in a particular project in Azkaban midgar-stg or midgar-prod.
#
#########################################

from HTMLParser import HTMLParser
import os
import requests

class parser(HTMLParser):

    def __init__(self, lookup_name):
        HTMLParser.__init__(self)
        self.lookup_name = lookup_name
        self.name_is_correct = False

    def handle_data(self, data):
        if data == self.lookup_name:
            self.name_is_correct = True

class projectIdParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.proj_id_found = False
        self.proj_id = -1

    def handle_data(self, data):
        if 'projectId' in data:
            # this logic relies on the fact that the line will be formatted as: "projectId = xxx;", where xxx is a string representing the projectId number
            data_list = data.split(" ")
            #get the index of the project id
            self.proj_id = data_list[data_list.index("projectId")+2]
            a = self.proj_id.split("\n")
            self.proj_id = a[0]
            #remove the ";" from the end of the string and cast as an int
            self.proj_id = int(self.proj_id[:-1])
            self.proj_id_found = True

# Authenticate with Azkaban and return response payload
def login_request(host, username, password):
    post_data = { "action" : "login", "username" : username, "password" : password }
    return requests.post(host, post_data)

def getHTMLsources(host, username, password, project_name):

    payload = {
        'action' : 'login',
        'username': username,
        'password': password,
    }

    session1 = requests.Session()
    session1.post(host, data = payload)
    req1 = session1.get(host)

    # create a new session because of a session reuse bug in requests pkg
    session2 = requests.Session()
    session2.post(host, data = payload)
    req2 = session2.get(host+'manager?project='+project_name)

    return [req1.content, req2.content]

def main():
  
  # Source these variables from the environment. In a typical
  # workflow they will be passed into the environment from Jenkins
  azkaban_host    = os.environ['AZKABAN_HOST_NAME']
  azkaban_flow    = os.environ['AZKABAN_FLOW_NAME']
  azkaban_proj    = os.environ['AZKABAN_PROJECT_NAME']
  azkaban_user    = os.environ['AZKABAN_USERNAME']
  azkaban_pass    = os.environ['AZKABAN_PASSWORD']
  target_date     = os.environ.get('AZKABAN_TARGET_DATE', None)
  failure_email   = os.environ.get('AZKABAN_FAILURE_EMAIL', 'none')
  success_email   = os.environ.get('AZKABAN_SUCCESS_EMAIL', 'none')
  disabled_steps  = os.environ.get('AZKABAN_DISABLED_STEPS', [])

  #List with two elements: [0] is the page source with all projects, [1] is the page source of the specific project
  sources = getHTMLsources(azkaban_host, azkaban_user, azkaban_pass, azkaban_proj)

  project_name_parser = parser(azkaban_proj)
  project_name_parser.feed(sources[0])

  if not project_name_parser.name_is_correct:
      print "Password or project name was not given correctly."
      return

  flow_name_parser = parser(azkaban_flow)
  flow_name_parser.feed(sources[1])

  if not flow_name_parser.name_is_correct:
      print "Flow name was not given correctly."
      return

  id_parser = projectIdParser()
  id_parser.feed(sources[1])

  if id_parser.proj_id_found:
      azkaban_proj_id = id_parser.proj_id

  print """
    azkaban_host:\t %s
    azkaban_flow:\t %s
    azkaban_proj:\t %s
    azkaban_proj_id:\t %s
    azkaban_user:\t %s
    azkaban_pass:\t ***************
    target_date:\t %s
    failure_email:\t %s
    success_email:\t %s
    disable:\t %s
  """ % (azkaban_host, azkaban_flow, azkaban_proj, azkaban_proj_id, azkaban_user, target_date, failure_email, success_email, disabled_steps)

  login_resp      = login_request(azkaban_host, azkaban_user, azkaban_pass)

  cookie_header   = "azkaban.browser.session.id=" + login_resp.json()['session.id'] + "; azkaban.failure.message=; azkaban.success.message="
  auth_header     = { "Cookie" : cookie_header } 

  if success_email != "none":
    successEmailsOverride = True
  else:
    successEmailsOverride = False

  # Flow execution parameters
  run_params = {
      'projectId' : azkaban_proj_id,
      'project' : azkaban_proj,
      'ajax' : 'executeFlow',
      'flow' : azkaban_flow,
      'failureEmailsOverride' : True,
      'successEmailsOverride' : successEmailsOverride,
      'failureAction' : 'finishPossible',
      'failureEmails' : failure_email,
      'successEmails' : success_email,
      'notifyFailureFirst' : True,
      'notifyFailureLast' : False,
      'concurrentOption' : 'pipeline',
      'pipelineLevel' : 1,
      'disabled' : disabled_steps
  }

  if target_date != "None":
      run_params['flowOverride[job.targetDate]'] = target_date

  run_response = requests.get(azkaban_host + "executor", headers=auth_header, params=run_params)

  # what's the status
  run_response.raise_for_status()

main()
