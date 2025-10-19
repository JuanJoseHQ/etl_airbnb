import datetime
import os

class Logs:
    """
    Clase para manejo de logs en archivos de texto.
    Permite registrar mensajes informativos y de error con timestamp.
    """

    def __init__(self, log_file="logs.txt"):
        """
        Inicializa el manejador de logs.
        - Crea el archivo de log si no existe.
        - Define la ruta/nombre del archivo donde se registrarán los mensajes.

        Parámetros:
        -----------
        log_file : str, opcional
            Nombre o ruta completa del archivo de logs (por defecto: "logs.txt").
        """
        self.log_file = log_file

        # Crear archivo de log si no existe
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", encoding="utf-8") as f:
                f.write("=== LOGS DE APLICACIÓN ===\n\n")

    def write_log(self, level, message):
        """
        Método interno que escribe un mensaje en el archivo de logs con fecha y tipo.

        Parámetros:
        -----------
        level : str
            Nivel del log, típicamente "INFO" o "ERROR".
        message : str
            Mensaje a registrar en el log.

        Comportamiento:
        ---------------
        - Añade un timestamp en formato 'YYYY-MM-DD HH:MM:SS'.
        - Escribe el mensaje en el archivo definido al inicializar la clase.
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] [{level}] {message}\n"

        # Abrir el archivo en modo append y escribir el log
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_message)

    def info(self, message):
        """
        Escribe un mensaje informativo (INFO) en el archivo de logs.

        Parámetros:
        -----------
        message : str
            Texto del mensaje informativo a registrar.
        """
        self.write_log("INFO", message)

    def error(self, message):
        """
        Escribe un mensaje de error (ERROR) en el archivo de logs.

        Parámetros:
        -----------
        message : str
            Texto del mensaje de error a registrar.
        """
        self.write_log("ERROR", message)
