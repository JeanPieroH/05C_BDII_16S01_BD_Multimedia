import librosa
import numpy as np
import os

def extract_features(audio_path):
    """
    Extrae características de un archivo de audio.

    Args:
        audio_path (str): Ruta al archivo de audio.

    Returns:
        np.ndarray: Vector de características.
    """
    try:
        # Completa la ruta del archivo de audio si solo se proporciona el nombre
        if not os.path.isabs(audio_path) and not audio_path.startswith("backend/database/sounds/"):
            audio_path = os.path.join("backend/database/sounds/", audio_path)

        y, sr = librosa.load(audio_path)

        # Extraer MFCCs
        mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13)
        mfccs_mean = np.mean(mfccs.T, axis=0)

        # Extraer deltas de MFCCs
        delta_mfccs = librosa.feature.delta(mfccs)
        delta_mfccs_mean = np.mean(delta_mfccs.T, axis=0)

        # Extraer deltas-deltas de MFCCs
        delta2_mfccs = librosa.feature.delta(mfccs, order=2)
        delta2_mfccs_mean = np.mean(delta2_mfccs.T, axis=0)

        # Concatenar todas las características
        features = np.concatenate((mfccs_mean, delta_mfccs_mean, delta2_mfccs_mean))

        return features
    except Exception as e:
        print(f"Error extracting features from {audio_path}: {e}")
        return None
