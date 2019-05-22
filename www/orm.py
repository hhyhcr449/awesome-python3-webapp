import aiomysql
import logging
logging.basicConfig(level=logging.INFO)  # INFO：确认一切按预期运行


def log(sql, args=()):
    # 下面用到log函数的地方都要注意，输出的这些信息能让你知道这个时间点程序在干什么
    logging.info('SQL: %s' % sql)


async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    # 声明变量__pool是一个全局变量，这样可以被其他函数引用
    global __pool
    __pool = await aiomysql.create_pool(
        # kw应该就是create_pool函数的参数**kw，也就是关键字参数
        # 下面就是将创建数据库连接需要用到的一些参数，从**kw中取出来
        # kw好象是是根据**kw创建的一个dict
        # kw的这个get函数的作用应该是，当没有传入host参数是就去默认值localhost
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],  # 这个就是dict的取值方式，不用多说了吧
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),  # 最大连接数10 ，最小连接数1
        minsize=kw.get('minsize', 1),
        loop=loop  #
    )

# 将执行sql的代码封到select函数中，调用的时候只需要传入sql，和sql需要的一些参数值就好


async def select(sql, args, size=None):
    log(sql, args)
    # 声明变量__pool是一个全局变量，这样才可以引用create_pool函数创建的变量__pool
    global __pool
    async with __pool.get() as conn:  # 从连接池中获取一个 数据库连接

        async with conn.cursor(aiomysql.DictCursor) as cur:
            # conn.cursor相当于是命令行下输入mysql -uroot -p之后进入到数据库中，cur就是那个不断闪烁的光标
            # sql.replace的作用是把sql的字符串占位符？换成python的占位符%s
            # args是执行sql语句时通过占位符插入的一些参数
            # 通过await语法来挂起自身的协程，并等待另一个协程完成直到返回结果
            await cur.execute(sql.replace('?', '%s'), args or ())
            # size:需要返回的结果数,如果不传入，就返回所有结果
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
            logging.info('rows returned：%s' % len(rs))
            return rs

# autocommit：自动提交


async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                # 受影响的行数，比如说插入一行数据，那受影响行数就是一行
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        # 数据库错误
        except BaseException as e:
            if not autocommit:
                await conn.rollback()

            raise
        return affected

# 这个函数在元类中被引用，作用是创建一定数量的占位符


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    # 比如说num=3，那L就是['?','?','?']，通过下面这句代码返回一个字符串'?,?,?'
    return ', '.join(L)

# 定义字段基类，后面各种各样的字段类都继承这个基类


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name  # 字段名
        self.column_type = column_type  # 字段类型
        self.primary_key = primary_key  # 主键
        self.default = default  # 默认值

    # 元类那节也有一个orm的例子，里面也有这个函数，好像是为了在命令行按照'<%s, %s:%s>'这个格式输出字段的相关信息
    # 注释掉之后会报错，不知道什么原因
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__,
                                self.column_type, self.name)


# 这部分内容会在models.py中引用
class StringField(Field):
    # ddl是数据定义语言("data definition languages")，默认值是'varchar(100)'，意思是可变字符串，长度为100
    # 和char相对应，char是固定长度，字符串长度不够会自动补齐，varchar则是多长就是多长，但最长不能超过规定长度
    def __init__(
            self,
            name=None,
            primary_key=False,
            default=None,
            ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


class ModelMetaclass(type):
    # name是当前类的类名，bases是当前类继承的父类集合，attrs是当前类的属性集合，
    # 元类的作用就是操作当前类的属性集合然后生成一个新的属性集合
    def __new__(cls, name, bases, attrs):
        # 排除Model类自身
        if name == 'Model':
            # 这里的意思是，如果是Model类就直接返回了，不需要定义下面的东西
            return type.__new__(cls, name, bases, attrs)
        # tableName就是需要在数据库中对应的表名，如果User类中没有定义__table__属性，那默认表名就是类名，也就是User
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = dict()  # 创建一个空的dict是为了后面储存User类的属性
        fields = []  # fields用来储存User类中除主键外的属性名
        primaryKey = None  # 主键默认为None，后面找到主键之后再赋值

        # attrs是User类的属性dict，需要通过items函数转换为[(k1,v1),(k2,v2)]这种形式，才能用for k, v
        # in来循环
        for k, v in attrs.items():
            if isinstance(v, Field):  # 检测v的类型是不是Field
                logging.info('  found mapping: %s ==> %s' % (k, v))

                mappings[k] = v
                if v.primary_key:  # 如果该字段的主键值为True，那就找到主键了
                    if primaryKey:  # 在主键不为空的情况下又找到一个主键就会报错，因为主键有且仅有一个
                        raise Exception(
                            'Duplicate primary key for field: %s' % k)
                    primaryKey = k  # 为主键赋值
                else:  # 不是主键的属性名储存到fields中
                    fields.append(k)
        if not primaryKey:  # 这就表示没有找到主键，也要报错，因为主键一定要有
            raise Exception('Primary key not found.')
        for k in mappings.keys():  # 把User类中原有的属性全部删除
            attrs.pop(k)
        # fields中的值都是字符串，下面这个匿名函数的作用是在字符串两边加上``生成一个新的字符串，为了后面生成sql语句做准备
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 把mappings这个dict存入attrs这个dict中
        # 其实attrs本来可能就有__table__属性的，但前面attrs.pop(k)把attrs里面的东西全给删了，所以这里需要重新赋值
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey  # 存入主键属性名
        attrs['__fields__'] = fields  # 存入主键外的属性名
        # 下面四句就是生成select、insert、update、delete四个sql语句，然后分别存入attrs
        # 要理解下面四句代码，需要对sql语句格式有一定的了解，其实并不是很难
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (
            primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(
            escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(
            map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (
            tableName, primaryKey)

        return type.__new__(cls, name, bases, attrs)

# 到这儿可以总结一下元类到底干了些什么，还是以User类为例
# 首先、元类找出User类在数据库中对应的表名，对User类的自有属性逐条进行分析，找出主键和非主键，同时把这些属性全部存入mappings这个dict
# 然后、删除User类的全部属性，因为实际操作数据库的时候用不到这些属性
# 最后、把操作数据库需要用到的属性添加进去，这包括所有字段和字段类型的对应关系，类对应的表名、主键名、非主键名，还有四句sql语句
# 这些属性才是操作数据库正真需要用到的属性，但仅仅只有这些属性还是不够，因为没有方法
# 而Model类就提供了操作数据库要用到的方法

# Model从dict继承，所以具备所有dict的功能，同时又实现了特殊方法__getattr__()和__setattr__()，因此又可以像引用普通字段那样写


class Model(dict, metaclass=ModelMetaclass):
    # 定义Model类的初始化方法
    def __init__(self, **kw):
        # 这里直接调用了Model的父类dict的初始化方法，把传入的关键字参数存入自身的dict中
        super(Model, self).__init__(**kw)
    # 有这个方法就可以通过属性来获取值，也就是d.k

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
    # 和上面一样，不过这个是通过d.k=v的方式

    def __setattr__(self, key, value):
        self[key] = value
    # 下面的getattr是用来获取当前实例的属性值，不要搞混了

    def getValue(self, key):
        # 如果没有与key相对应的属性值则返回None
        return getattr(self, key, None)
    # 如果当前实例没有与key对应的属性值时，就需要调用下面的方法了

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            # 当前实例找不到想要的属性值时，就要到__mappings__属性中去找了，__mappings__属性对应的是一个dict，这个前面提过了
            field = self.__mappings__[key]
            if field.default is not None:  # 如果查询出来的字段具有default属性，那就检查default属性值是方法还是具体的值
                # 如果是方法就直接返回调用后的值，如果是具体的值那就返回这个值
                value = field.default() if callable(field.default) else field.default
                logging.debug(
                    'using default value for %s: %s' %
                    (key, str(value)))
                # 查到key对应的value后就设置为当前实例的属性，是为了方便下次查询？不是很确定
                setattr(self, key, value)
        return value

    # 添加类方法，作用是让所有子类都能调用类方法
    @classmethod  # 这个装饰器是类方法的意思，这样就可以不创建实例直接调用类的方法
    # select操作的情况比较复杂，所以定义了三种方法
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '  # 通过条件来查询对象，一个对象对应数据库表中的一行
        # 有同学说cls就相当与是self，我感觉对象用self代表自己，类用cls代表自己，个人看法仅供参考
        sql = [cls.__select__]
        if where:  # 如果有where条件就在sql语句中加入字符串'where'和变量where
            sql.append('where')
            sql.append(where)
        if args is None:  # 这个参数是在执行sql语句前嵌入到sql语句中的，如果为None则定义一个空的list
            args = []
        orderBy = kw.get('orderBy', None)  # 从**kw中取得orderBy的值，没有就默认为None
        if orderBy:  # 解释同where
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')  # sql中limit有两种用法
            if isinstance(limit, int):  # 如果limit为一个整数n，那就将查询结果的前n个结果返回
                sql.append('?')
                args.append(limit)
            # 如果limit为一个两个值的tuple，则前一个值代表索引，后一个值代表从这个索引开始要取的结果数
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                # 用extend是为了把tuple的小括号去掉，因为args传参的时候不能包含tuple
                args.extend(limit)
            else:
                raise ValueError(
                    'Invalid limit value: %s' %
                    str(limit))  # 如果不是上面两种情况，那就一定出问题了
        rs = await select(' '.join(sql), args)  # sql语句和args都准备好了就交给select函数去执行
        return [cls(**r) for r in rs]  # 将查询到的结果一一返回，看不懂cls(**r)的用法，虽然能猜出这是个什么

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '  # 根据where条件查询结果数，注意，这里查询的是数量
        sql = [
            'select %s _num_ from `%s`' %
            (selectField, cls.__table__)]  # 这sql语句是直接重构的，不是调用属性，看不懂_num_是什么意思
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:  # 如果查询结果数为0则返回None
            return None
        # rs应该是个list，而这个list的第一项对应的应该是个dict，这个dict中的_num_属性值就是结果数，我猜应该是这样吧
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '  # 根据主键查找是最简单的，而且结果只有一行，因为主键是独一无二的
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    # save、update、remove这三个方法需要管理员权限才能操作，所以不定义为类方法，需要创建实例之后才能调用
    async def save(self):
        # 把实例的非关键字属性值全都查出来然后存入args这个list
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(
            self.getValueOrDefault(
                self.__primary_key__))  # 把主键找出来加到args这个list的最后
        rows = await execute(self.__insert__, args)  # 执行sql语句后返回影响的结果行数
        if rows != 1:  # 一个实例只能插入一行数据，所以返回的影响行数一定为1,如果不为1那就肯定错了
            logging.warning(
                'failed to insert record: affected rows: %s' %
                rows)
    # 下面两个的解释同上

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning(
                'failed to update by primary key: affected rows: %s' %
                rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning(
                'failed to remove by primary key: affected rows: %s' %
                rows)
