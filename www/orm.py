#!/usr/bin/env python3
# -*- coding: utf-8 -*- 

__autor__ = 'Dronly'

import asyncio
import logging
import aiomysql

# 创建连接池， 由__pool储存连接， 避免频繁打开和关闭数据库链接， 自动提交事务
@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

@asyncio.coroutine
def destory_pool():
    global __pool
    if __pool is not None:
        __pool.close()
        yield from __pool.wait_closed()


@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

# insert update delete 使用通用execute()函数   三种方法参数相同
def exectue(sql, args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.exectue(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

def create_args_string(num):
    L=[]
    for n in range(num):
        L.append('?')
    return ','.join(L)

class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='bigint'):
        super().__init__(name, ddl, primary_key, default)


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # 排除Model类本身：
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)

        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))

        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(' found mappings: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s'% k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields

        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) value (%s)' % (tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s` =?' % (tableName, ','.join(map(lambda f: '`%s` =?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s` =?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return __getattr__(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s:%s' % (key, str(value)))
                setattr(self, key, value)
            return value

    @classmethod
    @asyncio.coroutine
    def find(cls, pk):
        'find object by primary key.'
        rs = yield from select('%s where `%s` =?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, sef.fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__init__, args)
        if rows != 1:
            logging.warn('failed to insert record: arrected rows: %s' % rows)

    @classmethod
    @asyncio.coroutine
    def findAll(cls, **kw):
        rs = []
        if len(kw) == 0:
            rs = yield from select(cls.__select__, None)
        else:
            args=[]
            value=[]
            for k, v in kw.items():
                args.append('%s=?' % k)
                values.append(v)
        rs = yield from select('%s where %s' % (cls.select__, 'and'.join(args)), value)
        return rs

    @classmethod
    @asyncio.coroutine
    def update(cls, **kw):
        rs = []
        args = []
        values = []
        primary = kw.get(cls.__primary_key__)
        kw.pop(cls.__primary_key__)
        print(122, kw)
        for k, v in kw.items():
            value.append(v)
        values.append(primary)
        print('update', cls.__update__, kw.get(cls.__primary_key__))
        ret = yield from execute('%s' % (cls.__update__), values)
        return ret

    @classmethod
    @asyncio.coroutine
    def remove(cls, **kw):
        primaryKey = kw.get(cls.__primary_key__)
        ret = yield from execute('%s' % (cls.__delete__), primaryKey)
        return ret

class User(Model):
    __table__ = 'usres'

    id = IntegerField(primary_key = True)
    name = StringField()

    def show(self):
        print(1, '__mappings__:', self.__mappings__)


loop = asyncio.get_event_loop()

@asyncio.coroutine
def test():
    yield from create_pool(loop=loop, host='localhost', port=3306, user='root', password='123456', db='test')
    r = yield from User.findAll()


loop.run_until_complete(test())
loop.close()
#if loop.is_closed():
    #sys.exit(0)