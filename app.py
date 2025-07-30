
from typing import Any, Tuple, List
from simple_map_reduce import SimpleMapReduce
import io
import csv 



class WordCountMapReduce(SimpleMapReduce):
    """
    Ejemplo clásico: Contador de palabras usando MapReduce.
    """

    def map_function(self, key: Any, value: str) -> List[Tuple[Tuple[str,str],int]]:
        csv_file = io.StringIO(value)
        results = []
        try:
            csv_reader = csv.reader(csv_file, delimiter=',')
            columns = next(csv_reader)  
            last_name = columns[3].strip().lower()
            country = columns[6].strip().lower()
            print(f"{last_name}, {country}")
            results.append(((last_name, country), 1))
        except Exception as e:
            print(f"Error processing CSV: {e}")
        
        return results

    def reduce_function(self, key: Tuple[str, str], values: List[int]) -> List[Tuple[Tuple[str, str], int]]:
        total_count = sum(values)
        return [(key, total_count)]


# Ejemplo de uso
if __name__ == "__main__":

    with open("./data/custom.csv", "r", encoding="utf-8") as file:
        text = file.read()

#Creando chunks
chunks =  text.split("\n")
#Eliminamos el encabezado como tal del csv
chunks = chunks[1:]

documents = [(index,chunk)  for index, chunk in enumerate(chunks)]
    
print("EJEMPLO: Contador de Palabras con MapReduce")
print("=" * 60)
    
# Crear instancia del contador de palabras
word_counter = WordCountMapReduce()
    
    # Ejecutar MapReduce
results = word_counter.execute(documents)
#Encontrar los apellidos más comunes en USA
top_usa = [result for result in results if result[0][1]== "united states of america"]
top_usa = sorted(top_usa, key=lambda x: x[1], reverse=True)

    
    # Mostrar resultados finales
print("\nRESULTADOS FINALES:")
print("-" * 30)
for word, count in sorted(results, key=lambda x: x[1], reverse=True):
    print(f"{word}: {count}")


print('los apellidos más comunes en USA son:')
for (last_name, country), count in top_usa[:10]:
    print(f"{last_name}: {count}")