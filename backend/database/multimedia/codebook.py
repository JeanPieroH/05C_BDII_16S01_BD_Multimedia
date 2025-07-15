import numpy as np
import pickle
from sklearn.cluster import KMeans
from storage.HeapFile import HeapFile
from multimedia.feature_extraction import extract_features
from storage.Sound import Sound

def build_codebook(heap_file: HeapFile, field_name: str, num_clusters: int):
    """
    Construye un codebook a partir de las características de audio de una tabla.

    Args:
        heap_file (HeapFile): Instancia de HeapFile de la tabla.
        field_name (str): Nombre del campo de tipo SOUND.
        num_clusters (int): Número de clusters para K-Means.
    """
    all_features = []
    sound_handler = Sound(f"{heap_file.table_name}", field_name)
    for record in heap_file.get_all_records():
        sound_offset, _ = record.values[heap_file.schema.index((field_name, "SOUND"))]
        audio_path = sound_handler.read(sound_offset)
        features = extract_features(audio_path)  # No longer need to prepend path
        if features is not None:
            all_features.append(features)

    if not all_features:
        print("No features extracted, cannot build codebook.")
        return

    # Convertir a numpy array
    all_features = np.vstack(all_features)

    # Aplicar K-Means
    kmeans = KMeans(n_clusters=num_clusters, random_state=0, n_init=10).fit(all_features)

    # Crear el codebook
    codebook = {
        "centroids": kmeans.cluster_centers_,
        "doc_freq": np.zeros(num_clusters)
    }

    # Calcular la frecuencia de documentos
    for features in all_features:
        labels = kmeans.predict(features.reshape(1, -1))
        unique_labels = np.unique(labels)
        for label in unique_labels:
            codebook["doc_freq"][label] += 1


    # Guardar el codebook
    codebook_path = f"{heap_file.table_name}.{field_name}.codebook.pkl"
    with open(codebook_path, "wb") as f:
        pickle.dump(codebook, f)

    print(f"Codebook created and saved to {codebook_path}")
