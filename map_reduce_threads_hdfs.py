from simple_map_reduce_hdfs import SimpleMapReduceHDFS
from threaded_world_count_csv import ThreadedWordCountMapReduce

#Trabajo de hilos con HDFS
class HybridMapReduce(SimpleMapReduceHDFS):
    def __init__(self, num_threads=None):
        super().__init__()
        self.threaded_mapreduce = ThreadedWordCountMapReduce(num_threads=num_threads)

    def map_function(self, key, value):
        return self.threaded_mapreduce.map_function(key, value)

    def reduce_function(self, key, values):
        total_count = sum(values)
        return [(key, total_count)]

if __name__ == "__main__":
    with open("./data/custom.csv", "r", encoding="utf-8") as file:
        text = file.read()

    chunks = text.split("\n")[1:]
    documents = [(i, chunk) for i, chunk in enumerate(chunks) if chunk.strip()]

    hybrid_mr = HybridMapReduce(num_threads=10)
    results = hybrid_mr.execute(documents)

    # Filtra solo los resultados de USA
    usa_results = [((country, last_name), count) for ((country, last_name), count) in results if country == "united states of america"]

    usa_last_name_counts = {}
    for (country, last_name), count in usa_results:
        usa_last_name_counts[last_name] = usa_last_name_counts.get(last_name, 0) + count


    # Top 10 apellidos más comunes en USA
    top_10_usa = sorted(usa_last_name_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    print("\nTOP 10 Apellidos más comunes en USA:")
    print("-" * 30)
    for last_name, count in top_10_usa:
        print(f"{last_name}: {count}")
