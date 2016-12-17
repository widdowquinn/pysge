#!/usr/bin/python
###############################################################################
# pysge.py
#
# A module for submitting a number of command-line jobs to SGE, based on the
# following package:
#
# sge.py
#
#-----------------------------------------------------------------------
# ORIGINAL COMMENTS
# Creative Commons Attribution License
# http://creativecommons.org/licenses/by/2.5/
#
# Trevor Strohman
#    First release: December 2005
#    This version:  11 January 2006
#
# Bug finders: Fernando Diaz
#
# The above-named package had the following documentation on the page:
#
# http://ciir.cs.umass.edu/~strohman/code/
#-----------------------------------------------------------------------
#
# This Python library is meant to allow submission of several jobs, and
# large parameter-sweep submissions to a Grid Engine cluster.
# Each individual cluster job is represented by a Job object, and these
# objects can have dependencies on other jobs.
# Parameter sweeps are handled by JobGroup objects, which take the
# command, parameter names and parameter values as arguments.
# Once a JobGroup, or a set of job objects has been created, the
# sge.build_and_submit_jobs function dispatches these jobs for execution
# on the cluster.
#
# The module automatically redirects stdout and stderr of the submitted jobs.
#
# sge.py (Requires Python 2.x, Sun Grid Engine 6.0)
#
# This module extends the interface, and adds documentation concerning the
# operation of the code.
#==============================================================================
#
#
###############################################################################


""" pysge.py

    Submits jobs to the Sun Grid Engine.

    Handles job dependencies, output redirection, and job script creation for
    parameter sweeps
"""

#------------------------------------------------------------------------------
# IMPORTS

# Builtins
import os
import time


#------------------------------------------------------------------------------
# GLOBALS

# We set a global to provide the correct command-line options for a given
# queue on the local machine

# Dictionary, keyed by queue alias, containing a string with the appropriate
# command-line option
local_queues = {'fast': ' -q fast.q -l fq=true ',
                'all': ' -q all.q -l all=true '}

#------------------------------------------------------------------------------
# CLASSES

#----------------------------------------------------------------------
# Job

class Job:
  """ Objects in this class represent individual jobs to be run via SGE.
  """
  def __init__(self, name, command, queue=None):
    """ Instantiates a Job object.

        o name           String describing the job (uniquely)

        o command        String, the valid shell command to run the job

        o queue          String, the SGE queue under which the job shall run
    """
    self.name = name                 # Unique name for the job
    self.queue = queue               # The SGE queue to run the job under
    self.command = command           # Command line to run for this job
    self.script = command            # 
    self.scriptPath = None           # Will hold path to the script file
    self.dependencies = []           # List of jobs that must be submitted
                                     # before this job may be submitted
    self.submitted = True            # Flag indicating whether the job has
                                     # already been submitted


  def add_dependency(self, job):
    """ Add the passed job to the dependency list for this JobGroup.  This
        JobGroup should not execute until all dependent jobs are completed

        o job             Job, job to be added to the JobGroup's dependency
                          list
    """
    self.dependencies.append(job)


  def remove_dependency(self, job):
    """ Remove the passed job from this JobGroup's dependency list

        o job             Job, job to be removed from the JobGroup's dependency
                          list
    """
    self.dependencies.remove(job)
    

  def wait(self):
    """ 
    """
    finished = False
    interval = 5
    while not finished:
      time.sleep(interval)
      interval = min( 2 * interval, 60 )
      finished = os.system( "qstat -j %s > /dev/null" % (self.name) )

#------------------------------------------------------------------------------
# class JobGroup

class JobGroup:
  """
  """
  def __init__(self, name, command, queue=None, arguments={}):
    """ Instantiate a JobGroup object.  JobGroups allow for the use of
        combinatorial parameter sweeps by using the 'command' and 'arguments'
        arguments.  

        o name              String, the JobGroup name

        o command           String, the command to be run, with arguments
                            specified

        o queue             String, the queue for SGE to use

        o arguments         Dictionary, the values for each parameter as
                            lists of strings, keyed by an identifier for
                            the command string

        For example, to use a command 'my_cmd' with the arguments
        '-foo' and '-bar' having values 1, 2, 3, 4 and 'a', 'b', 'c', 'd' in
        all combinations, respectively, you would pass

        command='my_cmd $SGE_TASK_ID -foo $fooargs -bar $barargs'
        arguments='{'fooargs': ['1','2','3','4'],
                    'barargs': ['a','b','c','d']}
    """
    self.name = name                  # Set JobQueue name
    self.queue = queue                # Set SGE queue to request
    self.command = command            # Set command string
    self.dependencies = []            # Create empty list for dependencies
    self.submitted = True             # Set submitted Boolean
    self.arguments = arguments        # Dictionary of arguments for command
    self.generate_script()             # Make the SGE script for the parameter
                                      # sweep/array

  def generate_script(self):
    """ Create the SGE script that will run the jobs in the JobGroup, with the
        passed arguments
    """
    self.script = ""        # Holds the script string
    total = 1               # total number of jobs in this group

    # for now, SGE_TASK_ID becomes TASK_ID, but we base it at zero
    self.script += """let "TASK_ID=$SGE_TASK_ID - 1"\n"""

    # build the array definitions
    for key in self.arguments.keys():
      values = self.arguments[key]
      line = ("%s_ARRAY=( " % (key))
      for value in values:
        line += value
        line += " "
      line += " )\n"
      self.script += line
      total *= len(values)
    self.script += "\n"

    # now, build the decoding logic in the script
    for key in self.arguments.keys():
      count = len(self.arguments[key])
      self.script += """let "%s_INDEX=$TASK_ID %% %d"\n""" % ( key, count )
      self.script += """%s=${%s_ARRAY[$%s_INDEX]}\n""" % ( key, key, key )
      self.script += """let "TASK_ID=$TASK_ID / %d"\n""" % ( count )

    # now, add the command to run the job
    self.script += "\n"
    self.script += self.command
    self.script += "\n"

    # set the number of tasks in this group
    self.tasks = total


  def add_dependency(self, job):
    """ Add the passed job to the dependency list for this JobGroup.  This
        JobGroup should not execute until all dependent jobs are completed

        o job             Job, job to be added to the JobGroup's dependency
                          list
    """
    self.dependencies.append(job)


  def remove_dependency(self, job):
    """ Remove the passed job from this JobGroup's dependency list

        o job             Job, job to be removed from the JobGroup's dependency
                          list
    """
    self.dependencies.remove(job)
    

  def wait( self ):
    """
    """
    finished = False
    interval = 5
    while not finished:
      time.sleep(interval)
      interval = min( 2 * interval, 60 )
      finished = os.system("qstat -j %s > /dev/null" % (self.name))


#------------------------------------------------------------------------------
# FUNCTIONS

def build_directories(directory):
  """ Constructs the subdirectories output, stderr, stdout, and jobs in the
      passed directory.  These subdirectories have the following roles:

      jobs             Stores the scripts for each job
      stderr           Stores the stderr output from SGE
      stdout           Stores the stdout output from SGE
      output           Stores output (if the scripts place the output here)

      o directory         String, path to the top-level directory for the
                          creation of the subdirectories
  """
  # If the root directory doesn't exist, create it
  if not os.path.exists(directory):
    os.mkdir(directory)
  # Now make the subdirectories
  subdirectories = ["output", "stderr", "stdout", "jobs"];
  directories = [os.path.join(directory, subdir) for subdir in \
                 subdirectories]
  [os.mkdir(dirname) for dirname in directories if \
   not os.path.exists(dirname)]


def build_job_scripts( directory, jobs ):
  """ Constructs the script for each passed Job in the jobs list
  """
  # Loop over the job list, creating each job script in turn, and then adding
  # scriptPath to the Job object
  for job in jobs:
    scriptPath = os.path.join( directory, "jobs", job.name )
    with open(scriptPath, 'w') as scriptFile:
      scriptFile.write( "#!/bin/sh\n#$ -S /bin/bash\n%s\n" % job.script)
    job.scriptPath = scriptPath

def extract_submittable_jobs( waiting ):
  """ Obtain a list of jobs that are able to be submitted from the passed
      list of pending jobs

      o waiting           List of Job objects
  """
  submittable = []            # Holds jobs that are able to be submitted
  # Loop over each job, and check all the subjobs in that job's dependency
  # list.  If there are any, and all of these have been submitted, then
  # append the job to the list of submittable jobs.
  for job in waiting:
    unsatisfied = sum([(subjob.submitted is False) for subjob in \
                       job.dependencies])
    if 0 == unsatisfied:
      submittable.append( job )
  return submittable

def submit_safe_jobs(directory, jobs):
  """ Submit the passed list of jobs to the SGE server, using the passed
      directory as the root for output.

      o directory           Strung, path to output directory

      o jobs                List of Job objects
  """
  # Loop over each job, constructing the SGE command-line based on job settings
  for job in jobs:
    job.out = os.path.join(directory, "stdout")
    job.err = os.path.join(directory, "stderr")
    # Add the job name, current working directory, and SGE stdout and stderr
    # directories to the SGE command line
    args = " -N %s " % (job.name)
    args += " -cwd "
    args += " -o %s -e %s " % (job.out, job.err)
    # If a queue is specified, add this to the SGE command line
    if job.queue != None and job.queue in local_queues:
      args += local_queues[job.queue]
      #args += "-q %s " % job.queue
    # If the job is actually a JobGroup, add the task numbering argument
    if isinstance( job, JobGroup ):
      args += "-t 1:%d " % ( job.tasks )
    # If there are dependencies for this job, hold the job until they are
    # complete
    if len(job.dependencies) > 0:
      args += "-hold_jid "
      for dep in job.dependencies:
        args += dep.name + ","
      args = args[:-1]
    # Build the qsub SGE commandline
    qsubcmd = ("/usr/nfs/sge_root/bin/lx24-x86/qsub %s %s" % (args,
                                                              job.scriptPath)) 
    #print qsubcmd                      # Show the command to the user
    os.system(qsubcmd)                  # Run the command
    job.submitted = True                # Set the job's submitted flag to True

def submit_jobs(directory, jobs):
  """ Submit each of the passed job to the SGE server, using the passed
      directory as root for output.

      o directory          String, path to output directory

      o jobs               List of Job objects
  """
  waiting = list(jobs)                 # List of jobs still to be done
  # Loop over the list of pending jobs, while there still are any
  while len(waiting) > 0:
    # extract submittable jobs
    submittable = extract_submittable_jobs(waiting)
    # run those jobs
    submit_safe_jobs(directory, submittable)
    # remove those from the waiting list
    map(waiting.remove, submittable)
    
		
def build_and_submit_jobs(directory, jobs):
  """ Submits the passes list of Job objects to SGE, placing the output in the
      passed root directory

      o directory           Root directory for SGE and job output

      o jobs                List of Job objects, describing each job to
                            be submitted
  """
  # If the passed set of jobs is not a list, turn it into one.  This makes the
  # use of a single JobGroup a little more intutitive
  if type(jobs) != type([1]):
    jobs = [jobs]
    
  # Build and submit the passed jobs
  build_directories(directory)       # build all necessary directories
  build_job_scripts(directory, jobs) # build job scripts
  submit_jobs(directory, jobs)       # submit the jobs to SGE
