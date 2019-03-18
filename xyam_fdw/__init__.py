###
### based on https://github.com/asya999/yam_fdw
###

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log2pg

from pymongo import MongoClient
from dateutil.parser import parse
from bson.objectid import ObjectId

from functools import partial

import time

import json

dict_traverser = partial(reduce, lambda x, y: x.get(y) if type(x) == dict else x)

def coltype_formatter(coltype, otype):
    if coltype in ('timestamp without time zone', 'timestamp with time zone', 'date'):
        return lambda x: x if hasattr(x, 'isoformat') else parse(x)
    elif otype=='ObjectId':
        return lambda x: ObjectId(x)
    else:
       return None

class Yamfdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(Yamfdw, self).__init__(options, columns)

        self.host_name = options.get('host', 'localhost')
        self.port = int(options.get('port', '27017'))

        self.user = options.get('user')
        self.password = options.get('password')

        self.db_name = options.get('db', 'test')
        self.collection_name = options.get('collection', 'test')

        self.conn = MongoClient(host=self.host_name,
                             port=self.port)

        self.auth_db = options.get('auth_db', self.db_name)

        if self.user:
            self.conn.userprofile.authenticate(self.user,
                                            self.password,
                                            source=self.auth_db)

        self.db = getattr(self.conn, self.db_name)
        self.coll = getattr(self.db, self.collection_name)

        self.debug = options.get('debug', False)

        # if we need to validate or transform any fields this is a place to do it
        # we need column definitions for types to validate we're passing back correct types
        # self.db.add_son_manipulator(Transform(columns))

        if self.debug: log2pg('collection cols: {}'.format(columns))

        self.stats = self.db.command("collstats", self.collection_name)
        self.count=self.stats["count"]
        if self.debug: log2pg('self.stats: {} '.format(self.stats))

        self.indexes={}
        if self.stats["nindexes"]>1:
            indexdict = self.coll.index_information()
            self.indexes = dict([(idesc['key'][0][0], idesc.get('unique',False))  for iname, idesc in indexdict.iteritems()])
            if self.debug: log2pg('self.indexes: {} '.format(self.indexes))

        self.fields = dict([(col, {'formatter': coltype_formatter(coldef.type_name, coldef.options.get('type',None)),
                             'options': coldef.options,
                             'path': col.split('.')}) for (col, coldef) in columns.items()])

        if self.debug: log2pg('self.fields: {} \n columns.items {}'.format(self.fields,columns.items()))

        self.pipe = options.get('pipe')
        if self.pipe:
            self.pipe = json.loads(self.pipe)
            if self.debug: log2pg('pipe is {}'.format(self.pipe))
        else:
            self.pkeys = [ (('_id',), 1), ]
            for f in self.fields: # calculate selectivity of each field (once per session)
                if f=='_id': continue
                # check for unique indexes and set those to 1
                if f in self.indexes and self.indexes.get(f): 
                   self.pkeys.append( ((f,), 1) )
                elif f in self.indexes:
                   self.pkeys.append( ((f,), min((self.count/10),1000) ) )
                else: 
                   self.pkeys.append( ((f,), self.count) )
    
    def build_spec(self, quals):
        Q = {}

        comp_mapper = {'=' : '$eq',
                       '>' : '$gt',
                       '>=': '$gte',
                       '<=': '$lte',
                       '<>': '$ne',
                       '<' : '$lt',
                       (u'=', True) : '$in',
                       (u'<>', False) : '$nin',
                       '~~': '$regex'
                      }

        # TODO '!~~', '~~*', '!~~*', other binary ones that are composable

        for qual in quals:
            val_formatter = self.fields[qual.field_name]['formatter']
            vform = lambda val: val_formatter(val) if val is not None and val_formatter is not None else val
            if self.debug: log2pg('vform {} val_formatter: {} '.format(vform, val_formatter))

            if self.debug: log2pg('Qual field_name: {} operator: {} value: {}'.format(qual.field_name, qual.operator, qual.value))

            if qual.operator in comp_mapper:
               comp = Q.setdefault(qual.field_name, {})
               if qual.operator == '~~': 
                  comp[comp_mapper[qual.operator]] = vform(qual.value.replace('%','.*'))
               else: 
                  comp[comp_mapper[qual.operator]] = vform(qual.value)
               Q[qual.field_name] = comp
               if self.debug: log2pg('Qual {} comp {}'.format(qual.operator, qual.value))
            else:
               log2pg('Qual operator {} not implemented for value {}'.format(qual.operator, qual.value))

        return Q

    def get_rel_size(self, quals, columns):
        width = len(columns) * min(24, (self.stats["avgObjSize"]/len(self.fields)))
        num_rows = self.count
        if self.pipe: num_rows=self.count*10
        else:
           if quals: 
              fields=[q.field_name for q in quals]
              if '_id' in fields: num_rows=1
              else: 
                  # this part can only be allowed if Q is indexed, otherwise very bad
                  fields=[q.field_name in self.indexes for q in quals]
                  if True in fields:
                      Q = self.build_spec(quals)
                      num_rows = self.coll.find(Q).count()
        return (num_rows, width)

    def get_path_keys(self):
        return getattr(self, 'pkeys', [])

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        fields, eqfields, pipe = self.plan(quals, columns)

        pipe.reverse()

        return [ str(op) for op in pipe ]

    def execute(self, quals, columns, sortkeys=None):

        fields, eqfields, pipe = self.plan(quals, columns)

        if self.debug: t0 = time.time()
        if self.debug: log2pg('Calling aggregate with {} stage pipe {} '.format(len(pipe),pipe))

        cur = self.coll.aggregate(pipe, cursor={})

        if self.debug: t1 = time.time()
        if self.debug: docCount=0
        if self.debug: log2pg('cur is returned {} with total {} so far'.format(cur,t1-t0))

        if len(fields) == 0:
            for res in cur:
                docCount=res['rows']
                break

            for x in xrange(docCount):
                if eqfields: yield eqfields
                else: yield {}
        else:
            for doc in cur:
                doc = dict([(col, dict_traverser(self.fields[col]['path'], doc)) for col in columns])
                doc.update(eqfields)
                yield doc
                if self.debug: docCount=docCount+1

        if self.debug: t2 = time.time()
        if self.debug: log2pg('Python rows {} Python_duration {} {} {}ms'.format(docCount,(t1-t0)*1000,(t2-t1)*1000,(t2-t0)*1000))

    def flush(self, matches):
        if len(matches)!=0:
            return [ {'$match': matches} ]
        else:
            return []

    def pushDown(self, op, inmatches):
        if len(op.keys())!=1:
            # cannot pushdown this operation
            return (self.flush(inmatches), {})
        for opt in op.keys():
            if opt == '$match':
                outmatches = inmatches.copy()
                outmatches.update(op['$match'])
                return ([], outmatches)
            elif opt == '$project':
                outmatches = {}
                for k in inmatches.keys():
                    v = op['$project'].get(k,True)
                    if v == True:
                        outmatches[k] = inmatches[k]
                    else:
                        outmatches[v[1:]] = inmatches[k]
                return ([op],outmatches)
            elif opt == '$unwind':
                v = op['$unwind']
                ni = {}
                outmatches = {}
                for k in inmatches.keys():
                    if k.startswith(v[1:]):
                        ni[k] = inmatches[k]
                    outmatches[k] = inmatches[k]
                return ([op]+self.flush(ni),outmatches)
            else:
                # cannot pushdown through other operations
                return ([op]+self.flush(inmatches), {})

    def optimize(self, input):
        output = []
        matches = {}
        for op in reversed(input):
            ops, matches = self.pushDown(op, matches)
            output = ops + output
        return self.flush(matches) + output

    def plan(self, quals, columns):

        # Base pipeline
        pipe = []
        if self.pipe: pipe.extend(self.pipe)

        # Project (rename fields)
        fields = dict([(k, True) for k in columns])
        projectFields={}
        for f in fields:
           if 'options' in self.fields[f] and 'mname' in self.fields[f]['options']:
               projectFields[f]='$'+self.fields[f]['options']['mname']
           else:
               projectFields[f]=fields[f]
        pipe.append( { "$project" : projectFields } )
        if self.debug: log2pg('projectFields: {}'.format(projectFields))

        # Match
        Q = self.build_spec(quals)
        if Q: pipe.append( { "$match" : Q } )
        if self.debug: log2pg('mathcFields: {}'.format(Q))

        # optimization 1: if columns include field(s) with equality predicate in query,
        # then we don't have to fetch it, as we add them back later
        eqfields = dict([ (q.field_name , q.value) for q in quals if q.operator == '=' ])
        for f in eqfields: fields.pop(f)

        if len(fields) == 0:
            # optimization 2: no fields need to be returned, just get counts
            pipe.append( { "$count": "rows" } )
        elif len(eqfields) > 0:
            # remove constant fields, that get added back later
            pipe.append( { "$project" : fields } )

        # push-down filters through user supplied pipeline
        pipe = self.optimize(pipe)

        return (fields, eqfields, pipe)
