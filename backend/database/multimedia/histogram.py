import numpy as np
import pickle
from multimedia.feature_extraction import extract_features

def build_histogram(audio_path, codebook):
    """
    Construye un histograma de palabras acústicas para un archivo de audio.

    Args:
        audio_path (str): Ruta al archivo de audio.
        codebook (dict): Codebook con los centroides.

    Returns:
        np.ndarray: Histograma de palabras acústicas.
    """
    features = extract_features(audio_path)  # Path handling is now in extract_features
    if features is None:
        return None

    # Predecir los clusters para cada descriptor
    from sklearn.metrics.pairwise import euclidean_distances
    distances = euclidean_distances(features.reshape(1, -1), codebook["centroids"])
    labels = np.argmin(distances, axis=1)


    # Construir el histograma
    histogram = np.zeros(len(codebook["centroids"]))
    for label in labels:
        histogram[label] += 1

    return histogram

def load_codebook(table_name, field_name):
    """
    Carga un codebook desde un archivo.

    Args:
        table_name (str): Nombre de la tabla.
        field_name (str): Nombre del campo.

    Returns:
        dict: Codebook.
    """
    codebook_path = f"{table_name}.{field_name}.codebook.pkl"
    try:
        with open(codebook_path, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        print(f"Codebook not found at {codebook_path}")
        return None
