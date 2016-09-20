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

import os, sys

assert (len(sys.argv) >= 3)
user = sys.argv[1]
dbname = sys.argv[2]

drop = len(sys.argv) == 4

if drop:
    delete_string = "psql -U " + str(user) + " -d " + str(dbname) + " -f drop_postgres.sql"
    os.system(delete_string)

create_string = "psql -U " + str(user) + " -d " + str(dbname) + " -f postgres.sql"
os.system(create_string)
