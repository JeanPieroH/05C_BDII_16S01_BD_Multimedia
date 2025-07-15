import numpy as np
import heapq
from storage.HeapFile import HeapFile
from storage.Sound import Sound
from storage.HistogramFile import HistogramFile
from multimedia.histogram import build_histogram, load_codebook
from multimedia.feature_extraction import extract_features

def tf_idf(tftd, dft, N):
    """
    Calcula el peso TF-IDF para un término.
    """
    if tftd == 0:
        return 0
    tf = 1 + np.log10(tftd)
    idf = np.log10(N / dft) if dft > 0 else 0
    return tf * idf

def cosine_similarity(vec1, vec2):
    """
    Calcula la similitud de coseno entre dos vectores.
    """
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    if norm_vec1 == 0 or norm_vec2 == 0:
        return 0
    return dot_product / (norm_vec1 * norm_vec2)

def knn_sequential_search(query_audio_path: str, heap_file: HeapFile, field_name: str, k: int):
    """
    Realiza una búsqueda k-NN secuencial en un campo de audio.
    """
    codebook = load_codebook(heap_file.table_name, field_name)
    if codebook is None:
        return []

    # Construir el histograma y el vector TF-IDF para la consulta
    query_histogram = build_histogram(query_audio_path, codebook)
    if query_histogram is None:
        return []

    N = heap_file.heap_size
    query_tfidf = np.zeros(len(codebook["centroids"]))
    for i, count in enumerate(query_histogram):
        if count > 0:
            query_tfidf[i] = tf_idf(count, codebook["doc_freq"][i], N)

    # Usar una cola de prioridad para mantener los k mejores resultados
    priority_queue = []

    sound_handler = Sound(heap_file.filename.replace(".dat", ""), field_name)
    histogram_handler = HistogramFile(heap_file.filename.replace(".dat", ""), field_name)

    for record in heap_file.get_all_records():
        sound_offset, histogram_offset = record.values[
            heap_file.schema.index((field_name, "SOUND"))
        ]

        if histogram_offset == -1:
            continue

        # Leer el histograma y construir el vector TF-IDF
        histogram = histogram_handler.read(histogram_offset)
        doc_tfidf = np.zeros(len(codebook["centroids"]))
        for centroid_id, count in histogram:
            doc_tfidf[centroid_id] = tf_idf(count, codebook["doc_freq"][centroid_id], N)

        # Calcular la similitud de coseno
        similarity = cosine_similarity(query_tfidf, doc_tfidf)

        # Mantener los k mejores resultados en la cola de prioridad
        if len(priority_queue) < k:
            heapq.heappush(priority_queue, (similarity, record.values[0]))
        else:
            heapq.heappushpop(priority_queue, (similarity, record.values[0]))

    # Devolver los k mejores resultados ordenados por similitud
    results = sorted(priority_queue, key=lambda x: x[0], reverse=True)

    # Obtener los registros completos
    final_results = []
    for similarity, record_id in results:
        final_results.append((heap_file.search_by_field("id", record_id)[0], similarity))

    return final_results
