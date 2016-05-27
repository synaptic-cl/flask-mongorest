from utils import is_object_id, isint
from bson import ObjectId


class Operator(object):
    op = 'exact'

    # Can be overridden via constructor.
    allow_negation = False

    def __init__(self, allow_negation=False):
        self.allow_negation = allow_negation

    # Lets us specify filters as an instance if we want to override the
    # default arguments (in addition to specifying them as a class).
    def __call__(self):
        return self

    def prepare_queryset_kwargs(self, field, value, negate):
        if negate:
            return {'__'.join(filter(None, [field, 'not', self.op])): value}
        else:
            return {'__'.join(filter(None, [field, self.op])): value}

    def apply(self, queryset, field, value, negate=False):
        kwargs = self.prepare_queryset_kwargs(field, value, negate)
        return queryset.filter(**kwargs)


class Ne(Operator):
    op = 'ne'


class Lt(Operator):
    op = 'lt'


class Lte(Operator):
    op = 'lte'


class Gt(Operator):
    op = 'gt'


class Gte(Operator):
    op = 'gte'


class Exact(Operator):
    op = 'exact'

    def prepare_queryset_kwargs(self, field, value, negate):
        # Using <field>__exact causes mongoengine to generate a regular
        # expresison query, which we'd like to avoid.
        if negate:
            return {'%s__ne' % field: value}
        else:
            return {field: value}


class IExact(Operator):
    op = 'iexact'


class In(Operator):
    op = 'in'

    def prepare_queryset_kwargs(self, field, value, negate):
        # only use 'in' or 'nin' if multiple values are specified
        if ',' in value:
            value = value.split(',')
            op = negate and 'nin' or self.op
        else:
            op = negate and 'ne' or ''
        if type(value) is list:
            for index, x in enumerate(value):
                if isint(x):
                    value[index] = int(x)
        elif isint(value):
            value = int(value)
        return {'__'.join(filter(None, [field, op])): value}


class InObjectId(Operator):
    op = 'in'

    def prepare_queryset_kwargs(self, field, value, negate):
        # only use 'in' or 'nin' if multiple values are specified
        if ',' in value:
            value = [ObjectId(x) for x in value.split(',') if is_object_id(x)]
            op = negate and 'nin' or self.op
        else:
            if is_object_id(value):
                value = ObjectId(value)
            op = negate and 'ne' or ''
        return {'__'.join(filter(None, [field, op])): int(value)}


class InDict(Operator):
    op = 'indict'

    def prepare_queryset_kwargs(self, field, value, negate):
        """
        Function: prepare_queryset_kwargs
        Summary: Prepara la query para los casos que se deban buscar dentro
        de una lista que posee diccionarios.
        Examples: key=[field_dict].[value]
        Attributes:
        Returns: query
        """
        # only use 'in' or 'nin' if multiple values are specified
        dict_field, vals = value.split('.')
        if ',' in vals:
            value = [x for x in vals.split(',') if x]
            for index, x in enumerate(value):
                if isint(x):
                    value[index] = int(x)
                else:
                    value[index] = x
        else:
            if isint(vals):
                vals = int(vals)
            value = [vals]
        return dict(__raw__={field + '.' + dict_field: {"$in": value}})


class Contains(Operator):
    op = 'contains'


class IContains(Operator):
    op = 'icontains'


class Startswith(Operator):
    op = 'startswith'


class IStartswith(Operator):
    op = 'istartswith'


class Endswith(Operator):
    op = 'endswith'


class IEndswith(Operator):
    op = 'iendswith'


class Boolean(Operator):
    op = 'exact'

    def prepare_queryset_kwargs(self, field, value, negate):
        if value == 'false':
            bool_value = False
        else:
            bool_value = True

        if negate:
            bool_value = not bool_value

        return {field: bool_value}
