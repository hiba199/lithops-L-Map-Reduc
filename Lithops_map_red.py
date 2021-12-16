from timeit import default_timer as timer
import lithops

iterdata = ['cos://cloud-object-storage-lithops-d7-cos/xxx.txt']   # Change-me


def my_map_function(obj):
    print('Bucket: {}'.format(obj.bucket))
    print('Key: {}'.format(obj.key))
    print('Partition num: {}'.format(obj.part))
    counter = {}
    data = obj.data_stream.read()

    for line in data.splitlines():
        for word in line.decode('utf-8').split():
            if word not in counter:
                counter[word] = 1
            else:
                counter[word] += 1

    return counter


def my_reduce_function(results):
    final_result = {}
    for count in results:
        for word in count:
            if word not in final_result:
                final_result[word] = count[word]
            else:
                final_result[word] += count[word]

    return final_result


if __name__ == "__main__":
   
    chunk_size = 4*1024**2  # 4MB
    start = timer()
    fexec = lithops.FunctionExecutor(log_level='INFO')
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function, obj_chunk_size=chunk_size)
    print(fexec.get_result())
    end = timer()

    print('Elapsed time in seconds:')
    print(end - start)
    """
    With one reducer for each object
    """
    print()
    print('Testing one reducer per object:')
    fexec = lithops.FunctionExecutor()
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function, obj_chunk_size=chunk_size,
                     reducer_one_per_object=True)
    print(fexec.get_result())
