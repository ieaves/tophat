class genericMapper():
    """
    A generic mapper class which accepts as input a set of transformations to apply through the apply_map function
    """
    def __init__(self, transformed_objects, *args, **kwargs):
        self.transformed_objects = transformed_objects

    def apply_map(self, itemList):
        return_obj = [{} for item in itemList]

        for transformer in self.transformed_objects:
            print('Applying transformation:', transformer.__str__())
            vals = transformer.mapper(itemList)
            [obj.update(vals[i]) for i, obj in enumerate(return_obj)]

        return self.validate_data(return_obj)

    def validate_data(self, list_data):
        return list_data


class ManyToManyMapper(genericMapper):
    """
    TO DO: move update_db into mapper and out of transformer,
    """
    def __init__(self, transformed_objects):
        super(ManyToManyMapper, self).__init__(transformed_objects)

    def apply_map(self, objects, *args, **kwargs):
        return_obj = [{} for item in objects]

        for i, transformer in enumerate(self.transformed_objects):
            print('Applying transformation:', transformer.__str__())
            new_objects = self.evaluate_transformer(transformer, objects)
            missing_keys = list(set(objects[0].keys()) - set(new_objects[0].keys()))
            [obj.update({k: new_objects[i][k] for k in missing_keys}) for i, obj in enumerate(objects)]

        return None  # self.validate_data(return_obj)

    def evaluate_transformer(self, transformer, objects):
        [objects[i].update(item) for i, item in enumerate(transformer.mapper(objects))]
        return objects
