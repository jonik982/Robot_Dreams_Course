import os

from job1.dal import local_disk, sales_api




def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    # TODO: implement me
    # 1. get data from the API
    # 2. save data to disk



    jsondata = sales_api.get_sales(date)



    file_name = "sales_" + date + ".json"



    raw_dir_local = os.path.join(raw_dir, file_name)

    folderpath = os.path.join(raw_dir)







    if os.path.exists(raw_dir_local):
        os.remove(raw_dir_local)



    if os.path.exists(folderpath):
            os.rmdir(folderpath)


    if not os.path.exists(folderpath):
        os.mkdir(folderpath)




    local_disk.save_to_disk(jsondata, raw_dir_local)


    pass


