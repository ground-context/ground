'''
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import sys, os

assert (len(sys.argv) >= 2)
dbname = sys.argv[1]

drop = len(sys.argv) == 3

if drop:
    command_string = "cqlsh -k " + str(dbname) + " -f drop_cassandra.cql"
    os.system(command_string)

command_string = "cqlsh -k " + str(dbname) + " -f cassandra.cql"
os.system(command_string)

print "Successfully reset Cassandra."
