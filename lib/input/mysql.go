// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package input

import (
	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMySQL] = TypeSpec{
		constructor: NewMySQL,
		description: `
Streams a MySQL binary replication log as a series of JSON events. Each event
includes top level fields for the unique event id, schema name, table name, 
event timestamp, event type, map of primary keys, and a row summary containing
a before and after image of the row. A sample event is shown below:

` + "``` json" + `
{
	"id": "mysql-bin.000003:670:test:foo:1",
	"key": {
		"id": 1
	},
	"row": {
		"after": {
			"created_at": "2018-08-23T21:32:05.839348Z",
			"id": 1,
			"title": "foo"
		},
		"before": {
			"created_at": "2018-08-23T21:32:05.839348Z",
			"id": 1,
			"title": "bar"
		}
	},
	"schema": "test",
	"table": "foo",
	"timestamp": "2018-08-23T15:32:05-06:00",
	"type":"update"
}
` + "``` json" + `

This input type requires a durable cache for persisting log file and
offsets. It is also recommended to disable any cache TTL functionality.

This input supports both single (default) and batch message types. To
enable batch mode by setting the ` + "`batch_size`" + ` config field
to a value that is greater than 1. When operating in batch mode, the 
buffering window can be adjusted by tuning the ` + "`buffer_timeout`" + `
config field (default value is ` + "`1s`" + `).

### Starting Position & Content filters

This input supports the following starting positions:
- **last synchronized**
	- The default starting position uses the last synchronized
	checkpoint. This position will take priority over all others. To
	disable, clear the cache for this consumer.
- **dump**
	- This method begins by dumping the current db(s) using ` + "`mysqldump`" + `.
	This requires the ` + "`mysqldump`" + `executable to be available on the
	current machine. To use this starting position, the ` + "`mysqldump_path`" + `
	config field must contain the path/to/mysqldump,
- **latest**
	- This method will begin streaming at the latest binlog position.
	Enable this position using the ` + "`latest`" + ` config field.

A list of databases to stream can be specified using the ` + "`databases`" + `
config field. A optional table filter can be specified using the ` + "`tables`" + `
config field as long as only a single database is specified.

### Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- mysql_log_name
- mysql_log_position
- mysql_server_id
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).
	`,
	}
}

//------------------------------------------------------------------------------

// NewMySQL creates a new NSQ input type.
func NewMySQL(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	cache, err := mgr.GetCache(conf.MySQL.Cache)
	if err != nil {
		return nil, types.ErrCacheNotFound
	}

	m, err := reader.NewMySQL(conf.MySQL, cache, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader(TypeMySQL, m, log, stats)
}

//------------------------------------------------------------------------------
