
from tophat.generalUtils import get_table_pk_name
# TO DO: refactor so that each transformer returns an iterator by default with helper functionality to return a list (perhaps)


class genericTransformer():
    """
    Translates the entity in old_key to new_key
    """
    def __init__(self, old_key, new_key, *args, **kwargs):
        self.new_key = new_key
        self.old_key = old_key

    def mapper(self, objects):
        return ({self.new_key: item} for item in self.get_relevant(objects))

    def get_relevant(self, objects):
        return (item[self.old_key] for item in objects)

    def __str__(self):
        return '__'.join(['genericTransformer', str(self.old_key), str(self.new_key)])


class oneToOneTransformer(genericTransformer):
    """
    Applies a generic transform to the entity in old_key to create new_key
    """
    def __init__(self, old_key, new_key, mapper_func, *args, **kwargs):
        self.mapper_func = mapper_func
        super(oneToOneTransformer, self).__init__(old_key, new_key, *args, **kwargs)

    def mapper(self, objects):
        return [{self.new_key: self.mapper_func(item)} for item in self.get_relevant(objects)]

    def __str__(self):
        return '__'.join(['oneToOneTransformer', str(self.old_key), str(self.new_key)])


class ManyToManyFieldTransformer():
    """
    Assumes it's been passed an object with 'pks' for each object
    """

    def __init__(self, from_table, related_table, *args, **kwargs):
        class table_repr():
            def get_keys(self, val, table):
                default = self.unique_pk_name
                if not val:
                    ret = (default, default)
                else:
                    ret = val if isinstance(val, list) else (val, default)
                return ret

            def __init__(self, table, val):
                self.unique_pk_name = get_table_pk_name(table)
                k1, k2 = self.get_keys(val, table)
                self.table = table
                self.key_in_objects = k1
                self.object_key_in_db = k2

                self.defined_pk_in_object = None

                self.pk = self.table._meta.pk.name
                self.unique_pk_name = get_table_pk_name(self.table)

        class through_table_repr():
            def __init__(self, from_table, related_table):
                rel_table = [rel_table.through for rel_table in related_table._meta.related_objects
                             if from_table == rel_table.related_model]
                table = rel_table[0]
                foreign_keys = [field for field in table._meta.fields if field.many_to_one]
                from_column = [field.column for field in foreign_keys if field.related_model == from_table][0]
                related_column = [field.column for field in foreign_keys if field.related_model == related_table][0]

                self.table = table
                self.from_column = from_column
                self.related_column = related_column

        self.from_table = table_repr(from_table, kwargs.get('from_unique_on'))
        self.related_table = table_repr(related_table, kwargs.get('related_unique_on'))

        self.through_table = through_table_repr(from_table, related_table)

    def mapper(self, objects):
        # assumes each object is unique on the from table
        objects, defined_pk_in_object = self._confirm_or_set_primary_keys(objects, self.from_table)
        setattr(self.from_table, 'defined_pk_in_object', defined_pk_in_object)
        objects, defined_pk_in_object = self._confirm_or_set_primary_keys(objects, self.related_table)
        setattr(self.related_table, 'defined_pk_in_object', defined_pk_in_object)

        relevant = self.get_relevant(objects)
        unpacked = self.unpack_relations(relevant)

        keys = [self.through_table.from_column, self.through_table.related_column]
        mapped = [self.through_table.table(**dict(zip(keys, items))) for items in unpacked]

        self.update_db(mapped)
        return objects

    def get_primary_keys(self, objects, table_repr):
        from django.db.models import Q
        ids = [obj[table_repr.key_in_objects] for obj in objects]
        ids = ids if not isinstance(ids[0], list) else [subval for val in ids for subval in val]
        ids = list(set(ids))

        q_obj = Q(**{'__'.join([table_repr.object_key_in_db, 'in']): ids})
        pks = {
            str(item[table_repr.object_key_in_db]): item[table_repr.pk]
            for item in table_repr.table.objects.filter(q_obj).values(table_repr.pk, table_repr.object_key_in_db)
        }
        return pks

    def _confirm_or_set_primary_keys(self, objects, table_repr):
        if table_repr.object_key_in_db != table_repr.unique_pk_name:
            primary_keys = self.get_primary_keys(objects, table_repr)

            defined_pk_in_object = table_repr.unique_pk_name
            if isinstance(objects[0][table_repr.key_in_objects], list):
                [obj.update({defined_pk_in_object: [primary_keys[key] for key in obj[table_repr.key_in_objects]]})
                 for obj in objects]
            else:
                [obj.update({defined_pk_in_object: primary_keys[obj[table_repr.key_in_objects]]})
                 for obj in objects]
        else:
            defined_pk_in_object = table_repr.key_in_objects
        return (objects, defined_pk_in_object)

    def update_db(self, objects):
        self.through_table.table.objects.bulk_create(objects)

    def unpack_relations(self, objects):
        return [(item[self.from_table.defined_pk_in_object], sub_item)
                for item in objects for sub_item in item[self.related_table.defined_pk_in_object]]

    def get_relevant(self, objects):
        return [{key: object[key] for key in (self.from_table.defined_pk_in_object, self.related_table.defined_pk_in_object)}
                for object in objects]


class OneToOneFieldTransformer(genericTransformer):
    """
    TO DO: Create a default strategy for handling missing foreign key entities rather than simply inserting null
    This could actually get pretty neat where it automatically create the foreign key entity if possible.
    """
    def __init__(self, old_key, new_key, mapper_func, *args, **kwargs):
        self.arg = kwargs.get('arg', 'pk')
        self.mapper_func = mapper_func
        super(OneToOneFieldTransformer, self).__init__(old_key, new_key, *args, **kwargs)

    def mapper(self, objects):
        itemList = self.get_relevant(objects)
        mapped_objs = {getattr(obj, self.arg): obj for obj in self.mapper_func(itemList)}
        return [{self.new_key: mapped_objs.get(obj[self.old_key], None)} for obj in objects]

    def get_relevant(self, objects):
        return list(set([item[self.old_key] for item in objects]))

    def __str__(self):
        return '__'.join(['foreignkeyTransformer', str(self.old_key), str(self.new_key)])


class compositeObjectTransformer(oneToOneTransformer):
    """
    Applies a generic transformation to the entities in composite_keys to create new_key
    """
    def __init__(self, composite_keys, new_key, mapper_func, *args, **kwargs):
        self.composite_keys = composite_keys
        super(compositeObjectTransformer, self).__init__(None, new_key, mapper_func, *args, **kwargs)

    def get_relevant(self, objects):
        return [[item[key] for key in self.composite_keys] for item in objects]

    def __str__(self):
        return '__'.join(['compositeObjectTransformer', self.new_key])


class chainedObjectTransformer():
    def __init__(self, needed_keys, transformerList, *args, **kwargs):
        self.transformerList = transformerList
        self.needed_keys = needed_keys

    def mapper(self, objects):
        new_objects = self.get_relevant(objects)
        for transformer in self.transformerList[:-1]:
            new_objects = self.evaluate_transformer(transformer, new_objects)
            # add any new keys available to the list.

        return self.transformerList[-1].mapper(new_objects)

    def get_relevant(self, objects):
        return [{key: item[key] for key in self.needed_keys} for item in objects]

    def evaluate_transformer(self, transformer, objects):
        [objects[i].update(item) for i, item in enumerate(transformer.mapper(objects))]
        return objects

    def __str__(self):
        return '__'.join(['chainedObjectTransformer'] + self.needed_keys)
