A simple utility class providing simple data slurping methods for CSV, JSON, and plain text data. 

Becomes sometimes Pandas is just too darn big (and slow to import) for what you need to do.

### License

Copyright 2017-2018 by the author (wstlabs).

Please see the file `LICENSE.txt` in this repository for licensing details.

### Status

Pre-alpha.  Has been exercised extensively on my end, but (to my knowledge) not by anyone else yet.

### Synopsis 

For example, to get a reasonably robust row iterator for a given file: 

```
import sys
import ioany

filepath = sys.argv[1]
rows = ioany.read_csv(filepath)
```

Or if you'd prefer an iterator of (ordered) dict structs: 
```
rows = ioany.read_csv(filepath)
```

and so forth.  

### Testing 

No unit tests.  Currently QA'd through extensive personal use only.

### Bugs & Caveats
* Python 3 only 
* Currently only tested in Unix-like envronments
* And on vanilla input files with vanilla structure (with regard to EOLN/EOF/BOM etc)
* Do you know what this means?

```
Copyright 2017-2018 wstlabs (https://github.com/wstlabs) 

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```




