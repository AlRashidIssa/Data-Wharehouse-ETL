import gdown, sys, os
from pathlib import Path
from abc import ABC, abstractmethod

# Define MAIN_DIR to point to the project root directory
MAIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(MAIN_DIR)
from utils import ErrorTrack, PipelineTrack

class ILoadFromDrive(ABC):
    """
    Abstract Base Class (ABC) for loading files from Google Drive.
    """

    @abstractmethod
    def load(self, url: str, save_archive: str, name: str) -> None:
        """
        Abstract method to be implemented for loading files from Google Drive.

        Parameters:
        -----------
        url (str): The Google Drive link for the file.
        save_archive (str): The directory path to save the downloaded file.
        name (str): The name to save the downloaded file as.

        Returns:
        --------
        None
        """
        pass


class LoadFromDrive(ILoadFromDrive):
    """
    Concrete class for downloading files from Google Drive.
    """

    def load(self, url: str, save_archive: str, name: str) -> None:
        """
        Downloads a file from Google Drive and saves it to the specified directory.

        Parameters:
        -----------
        url (str): The Google Drive link for the file to be downloaded.
        save_archive (str): The path to the directory where the file should be saved.
        name (str): The name to use for the saved file (without extension).

        Raises:
        -------
        TypeError: If any parameter is not a string.
        FileNotFoundError: If the save directory does not exist.
        ValueError: If the extracted file ID is invalid or missing.
        Exception: For any other unforeseen errors during the download process.

        Returns:
        --------
        None
        """
        # Parameter validation
        if not isinstance(url, str):
            error_msg = f"The URL must be a string. Provided type: {type(url)}"
            ErrorTrack(error_msg)
            raise TypeError(error_msg)

        if not isinstance(name, str):
            error_msg = f"The file name must be a string. Provided type: {type(name)}"
            ErrorTrack(error_msg)
            raise TypeError(error_msg)

        if not os.path.exists(save_archive):
            error_msg = f"The specified save directory does not exist: {save_archive}"
            ErrorTrack(error_msg)
            raise FileNotFoundError(error_msg)

        try:
            # Extract the file ID from the Google Drive URL
            try:
                file_id = url.split('/d/')[1].split('/')[0]
                download_link = f"https://drive.google.com/uc?id={file_id}"
            except IndexError:
                error_msg = f"Invalid Google Drive URL format: {url}"
                ErrorTrack(error_msg)
                raise ValueError(error_msg)

            # Prepare the full save path
            save_path = f"{save_archive}/{name}.zip"

            # Log the download process
            PipelineTrack(f"Starting download from Google Drive. File: {name}, Destination: {save_path}")

            # Download the file using gdown
            gdown.download(download_link, str(save_path), quiet=False)

            # Log successful download
            PipelineTrack(f"File downloaded successfully: {save_path}")

        except Exception as e:
            error_msg = f"An error occurred while downloading the file: {str(e)}"
            ErrorTrack(error_msg)
            raise Exception(error_msg) from e


if __name__ == "__main__":
    save_archive="/workspaces/Data-Wharehouse-ETL/database/archive"
    PipelineTrack("Strating Donwliading DataBast From google Drive")
    loader = LoadFromDrive().load(url="https://drive.google.com/file/d/139OEjxiFwxJtFQWaixF0fG4JSQyGp6EP/view?usp=sharing",
                                  save_archive=save_archive,
                                  name="database_v1")
    from utils.common_tools import get_size
    PipelineTrack(F"Comblited Download Database from google Drive. Note:Size={get_size(f"{save_archive}/database_v1.zip")}, You cna find the database in {save_archive}")
