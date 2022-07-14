import pandas as pd
from pyutils.database.github_database import GitHubDataBase

def get_root_database() -> GitHubDataBase:
    # Returns the main root database.
    return GitHubDataBase.restore_database("fintools_database",
            "Jacob-Pang", "fintools-database")

def get_database(database_id: str = None) -> GitHubDataBase:
    # Returns database by ID.
    root_database = get_root_database()

    if not database_id: # Returns the root database by default.
        return root_database
    
    child_database = root_database.get_child_node(database_id)
    return child_database

def get_entity_metadata(root_database: GitHubDataBase = get_root_database()) -> pd.DataFrame:
    return root_database.get_child_node("entity_metadata").read_data() \
            .set_index("Entity")

def get_entity_tracker(root_database: GitHubDataBase = get_root_database()) -> pd.DataFrame:
    return root_database.get_child_node("entity_tracker").read_data() \
            .set_index("Entity")

if __name__ == "__main__":
    pass