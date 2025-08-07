
from src.inventory.common_parser import create_and_save_techsmart_txt_file, save_checklist



def run_manual_adjustments(po, config, customer, files_save_path):
    po['Caja inicial'] = 1
    po_num = files_save_path.rsplit('/', 1)[-1]
    techsmart = create_and_save_techsmart_txt_file(po, customer, config, po_num, files_save_path)
    save_checklist(po, None, techsmart, config, po_num, files_save_path)


