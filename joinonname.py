from mrjob.job import MRJob
import itertools


def are_connected(way1, way2):
    return (
        way1['nodes'][-1] == way2['nodes'][0] or
        way1['nodes'][0] == way2['nodes'][0] or
        way1['nodes'][-1] == way2['nodes'][-1]
    )


def merge_tags(way1, way2):
    tags = {}
    for key in way1['tags']:
        if key in way2['tags'] and way1['tags'][key] == way2['tags'][key]:
            tags[key] = way1['tags'][key]

    return tags


def merge_ways(way1, way2):
    if way1['nodes'][-1] == way2['nodes'][0]:
        new_way = way1
        new_way['nodes'].extend(way2['nodes'][1:])
        new_way['tags'] = merge_tags(way1, way2)
    elif way1['nodes'][0] == way2['nodes'][0]:
        new_way = way1
        new_way['nodes'].extend(way2['nodes'][::-1][1:])
        new_way['tags'] = merge_tags(way1, way2)
    elif way1['nodes'][-1] == way2['nodes'][-1]:
        new_way = way1
        new_way['nodes'].extend(way2['nodes'][::-1][1:])
        new_way['tags'] = merge_tags(way1, way2)
    else:
        raise NotImplementedError(way1, way2)

    return new_way


def pair_that_match(inputlist, predicate):
    match = None
    for idx1, el1 in enumerate(inputlist):
        for idx2, el2 in enumerate(inputlist):
            if idx1 == idx2:
                continue
            if predicate(el1, el2):
                match = (idx1, idx2)
                break
        if match is not None:
            break

    if match is None:
        return (None, inputlist)
    else:
        return ((inputlist[match[0]], inputlist[match[1]]), [el for idx, el in enumerate(inputlist) if idx not in match])


def merge_fully(inputlist, predicate_to_merge, merge_func):
    while True:
        connected, other = pair_that_match(inputlist, predicate_to_merge)
        if connected is None:
            results = other
            break
        else:
            el1, el2 = connected
            inputlist = other + [merge_func(el1, el2)]

    return inputlist



class JoinWays(MRJob):
    def mapper_parse_input(self, _, line):
        words = line.split(" ")
        parts = {part[0]:part[1:] for part in words}

        parts['type'] = line[0]
        if 'x' in parts:
            parts['x'] = float(parts['x'])
            parts['y'] = float(parts['y'])
        if parts.get('T', '') != '':
            parts['tags'] = dict(kv.split("=") for kv in parts['T'].split(","))
        else:
            parts['tags'] = {}

        if 'N' in parts:
            parts['nodes'] = parts['N'].split(',')

        yield (words[0], parts)

    def mapper_only_ways(self, key, value):
        if value['type'] == 'w' and 'highway' in value['tags']:
            yield (key, {'type': 'w', 'tags': value['tags'], 'nodes': value['nodes']})

    def mapper_merge_by_ref(self, key, value):
        if 'ref' in value['tags']:
            yield (value['tags']['ref'], value)
        else:
            yield (None, value)

    def combiner_merge(self, key, values):
        values = list(values)

        if key is not None:
            values = merge_fully(values, are_connected, merge_ways)

        for value in values:
            yield (key, value)

    def reducer_merge(self, key, values):
        values = list(values)

        if key is not None:
            values = merge_fully(values, are_connected, merge_ways)

        for value in values:
            yield (key, value)


    def steps(self):
        return [
            self.mr(mapper=self.mapper_parse_input),
            self.mr(mapper=self.mapper_only_ways),
            self.mr(mapper=self.mapper_merge_by_ref, combiner=self.combiner_merge, reducer=self.reducer_merge),
        ]

if __name__ == '__main__':
    JoinWays.run()
