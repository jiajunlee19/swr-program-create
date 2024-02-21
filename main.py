try:
    import time
    import os
    import sys
    import getpass
    import pandas as pd
    import numpy as np
    import xml.etree.ElementTree as ETree
    from utils.logger import logger_init
    from utils.Common_Functions_64 import ExpandSeries, delete_file

except ImportError as IE:
    print(f"Import Error: {str(IE)}")
    time.sleep(5)


def init():
    '''init'''

    # Get path_main and transform into absolute path (so it works for onedrive path too)
    sharepoint_online_path = f"https://microncorp-my.sharepoint.com/personal/{getpass.getuser()}_micron.com/Documents/"
    sharepoint_local_path = f"C:\\Users\\{getpass.getuser()}\\OneDrive - Micron Technology, Inc\\"
    path_main = os.path.dirname(os.path.realpath(sys.argv[0]))
    path_main = path_main.replace(sharepoint_online_path, sharepoint_local_path)
    path_main = path_main.replace("/", "\\")

    # Define working folder paths
    path_recipe_bom = f"{path_main}\\recipe-bom"
    path_recipe_swr = f"{path_main}\\recipe-swr"
    filename_swr = 'SWR.xlsx'
    path_swr = f"{path_main}\\{filename_swr}"

    # Init logger
    try:
        df_settings = pd.read_excel(path_swr, sheet_name='settings')
        loglevel = list(df_settings['LOG_LEVEL'])[0]
        loglevel_error = False 
    except (ValueError, KeyError):
        loglevel_error = True
        loglevel = 'INFO'

    if loglevel_error:
        log.warning('LOG_LEVEL is not defined in settings, setting to INFO...')

    log = logger_init('SWR_PROGRAM_CREATE.log', f"{path_main}\\Log", 'w', loglevel)
    log.info(f"Running main.py in {path_main} with loglevel = {loglevel}")

    return log, path_main, path_recipe_bom, path_recipe_swr, path_swr


def main(log, path_main, path_recipe_bom, path_recipe_swr, path_swr):
    '''main'''
    
    log.info(f"path_recipe_bom = {path_recipe_bom}")
    log.info(f"path_recipe_swr = {path_recipe_swr}")
    log.info(f"path_swr = {path_swr}")

    # Read main excel workbook
    log.info('Reading swr file...')
    input_columns = ['CBID', 'PNP_PROGRAM_SIDE1', 'PNP_PROGRAM_SIDE2', 'PART NUMBER (IS)', 'PART NUMBER (WAS)', 'DESIGNATOR']

    # Create df_input for input sheet
    log.info('Creating dataframe for input sheet...')
    df_input = pd.read_excel(path_swr, sheet_name='SWR')
    log.debug(f"\n{df_input.head(5).to_string(index=False)}")

    log.debug('Trimming all input columns...')
    for input_column in input_columns:
        df_input[input_column] = df_input[input_column].astype(str)
        df_input[input_column] = df_input[input_column].str.strip().str.upper().str.lstrip('0')
    df_input = df_input.replace([' '], ['']).replace(['NAN'], ['']).replace([''], [np.NaN], regex=True)
    log.debug(f"\n{df_input.head(5).to_string(index=False)}")

    log.debug('Dropping null rows...')
    df_input.dropna(how='any', subset=['CBID', 'PNP_PROGRAM_SIDE1', 'PART NUMBER (IS)', 'PART NUMBER (WAS)', 'DESIGNATOR'], inplace=True)
    log.debug(f"\n{df_input.head(5).to_string(index=False)}")

    if len(df_input) < 1:
        raise ConnectionAbortedError ('There is no input to be processed, force exiting application...')

    log.debug('Dropping CBID duplicates...')
    df_input.drop_duplicates(subset=['CBID'], keep='first', inplace=True)
    log.debug(f"\n{df_input.head(5).to_string(index=False)}")

    log.info('Starting to loop through df_input...')
    for i in range(len(df_input)):

        try:

            log.info(f"Reading row #{i+1} with CBID = {df_input.loc[i, 'CBID']}, PNP_PROGRAM_SIDE1 = {df_input.loc[i, 'PNP_PROGRAM_SIDE1']}, PNP_PROGRAM_SIDE2 = {df_input.loc[i, 'PNP_PROGRAM_SIDE2']}")
            log.debug(f"\n{df_input.take([i]).to_string(index=False)}")

            try:
                selected_program = [df_input.loc[i, 'PNP_PROGRAM_SIDE1'], df_input.loc[i, 'PNP_PROGRAM_SIDE2']].remove('')
            except (KeyError, ValueError):
                selected_program = [df_input.loc[i, 'PNP_PROGRAM_SIDE1'], df_input.loc[i, 'PNP_PROGRAM_SIDE2']]

            # log.debug('Hardcoding selected files...')
            # selected_program = ['3440CB-PD0-M5-IT', '3440CB-SD0-M5-IT']

            log.info(f"Selected_program = {selected_program}")

            log.debug("Initializing file_program dicts...")
            file_program = {}

            # Recursively call scandir inclusive of subfolders for filename matching
            def scan_dir_file(path):
                for f in os.scandir(path):
                    if f.is_file() and (f.name[-3:].lower() == '.pp' or f.name[-4:].lower() == '.pp7') and any (matcher in f.name for matcher in selected_program):
                        yield f.path
                    elif f.is_dir():
                        yield from scan_dir_file(f.path)
            file_program = {f for f in scan_dir_file(path_recipe_bom)}
            file_program = sorted(file_program)

            log.info(f"Matched file_program for CBID = {df_input.loc[i, 'CBID']} is {file_program}")

            # Continue only if at least one program file is found
            if len(file_program) < 1:
                log.warning('There is no selected program file found.')
                log.warning(f"Force skipping row #{i+1} with CBID = {df_input.loc[i, 'CBID']}...")
                continue

            log.debug('Splitting part number and designator...')
            partIsList =  df_input.loc[i, 'PART NUMBER (IS)'].split('\n')
            partWasList =  df_input.loc[i, 'PART NUMBER (WAS)'].split('\n')
            designatorsList =  df_input.loc[i, 'DESIGNATOR'].split('\n')

            if len(partIsList) != len(partWasList) or len(partIsList) != len(designatorsList) or len(partWasList) != len(designatorsList):
                raise AssertionError(f"Number of partIs, partWas and designator does not tally, force skipping CBID = {df_input.loc[i, 'CBID']} !")

            if 'NO PLACE' in (x.strip().upper() for x in partWasList):
                raise AssertionError(f"NO PLACE is found in partWasList, unable to handle part addition, force skipping CBID = {df_input.loc[i, 'CBID']} !")

            log.info(f"Total of {len(designatorsList)} line item(s) to be processed for CBID = {df_input.loc[i, 'CBID']}.")

            # Loop through matched file_program
            for file in file_program:

                log.info(f"Processing: {file}...")

                filename = file.rsplit('\\', 1)[-1]
                log.debug(f"filename = {filename}")

                filename_without_ext = filename.rsplit('.', 1)[0]
                log.debug(f"filename_without_ext = {filename_without_ext}")

                file_ext = filename.rsplit('.', 1)[-1]
                log.debug(f"file_ext = {file_ext}")

                xmldata = file
                prstree = ETree.parse(xmldata)
                root = prstree.getroot()
            
                # Loop through line items to be processed
                for j in range(len(designatorsList)):
                    partIs, partWas, designators = partIsList[j].strip().upper(), partWasList[j].strip().upper(), designatorsList[j].strip().upper()
                    log.info(f"Processing partIs = {partIs}, partWas = {partWas}, designators = {designators} in {file} ...")
                    
                    # Expand and split designators
                    log.info(f"Expanding and spliting {designators} ...")
                    designators = ExpandSeries(designators)
                    if '-' in designators:
                        raise AssertionError(f"Designators {designators} not expanded and contains '-', force skipping CBID = {df_input.loc[i, 'CBID']} !")

                    designatorList = designators.split(',')
                    log.info(f"designatorList = {designatorList}")

                    # Handle .pp7 file
                    if file_ext.lower() == 'pp7':
                        pp_url = '{http://api.assembleon.com/pp7/v1}'
                        log.debug(f"Setting pp_url to {pp_url}...")

                        feeder_whitelist, check_designator, all_designator, componentToRemove, feederToRemove, actionToRemove, robotHeadToRemove = set(), set(), set(), set(), set(), set(), set()
                        for BoardInfo in root.iter(f"{pp_url}Board"):

                            # Modify program names for the 1st time processing the file only
                            if j == 0:
                                sPROGRAM_NAME = BoardInfo.attrib.get('id')
                                sPROGRAM_NAME_NEW = f"{sPROGRAM_NAME}-{df_input.loc[i, 'CBID']}"
                                log.info(f"Modifying program name from {sPROGRAM_NAME} to {sPROGRAM_NAME_NEW} ...")
                                BoardInfo.attrib['id'] = BoardInfo.attrib['id'].replace(sPROGRAM_NAME, sPROGRAM_NAME_NEW)

                            for ComponentInfo in BoardInfo.iter(f"{pp_url}Component"):
                                sPartNumber = ComponentInfo.attrib.get('partNumber')
                                sREFDES = ComponentInfo.attrib.get('refDes')

                                if sPartNumber == partWas and 'ALL' in designatorList: 
                                    log.debug(f"Adding {sREFDES} into all_designator...")
                                    all_designator.add(sREFDES)

                                elif sPartNumber == partWas and 'ALL' not in designatorList:
                                    log.debug(f"Adding {sREFDES} into check_designator...")
                                    check_designator.add(sREFDES)

                                for designator in designatorList:

                                    # Handle part removal, store all component info of the given component PN & REFDES to be deleted
                                    if partIs == 'NO PLACE' and sPartNumber == partWas and (sREFDES == designator or designator == 'ALL'):
                                        log.debug(f"Storing {sREFDES} {ComponentInfo} to be deleted in componentToRemove...")
                                        componentToRemove.add(ComponentInfo)

                                    # Handle part sub, modify the component part number of the given REFDES
                                    elif sPartNumber == partWas and (sREFDES == designator or designator == 'ALL'):
                                        log.info(f"Modifying {sREFDES} componenet part number from {partWas} to {partIs} ...")
                                        ComponentInfo.attrib['partNumber'] = ComponentInfo.attrib['partNumber'].replace(partWas, partIs)

                            # Handle part removal, delete all component in componentToRemove
                            for item in componentToRemove:
                                log.info(f"Deleting {item} in componentToRemove...")
                                BoardInfo.remove(item)
                            componentToRemove = set()

                        if len(all_designator) > 0:
                            log.info(f"Replacing designatorList with all_designator = {all_designator} ...")
                            designatorList = all_designator

                        elif len(check_designator) > 0:
                            log.info('Checking if all designators are included...')
                            for designator in designatorList:
                                if designator != 'ALL':
                                    try:
                                        check_designator.remove(designator)
                                    except:
                                        pass
                            if len(check_designator) > 0:
                                log.info(f"Not all designators are included, non-impacted designator = {check_designator}")
                            else:
                                log.info('All designators are included.')

                        for SegmentInfo in root.iter(f"{pp_url}Segment"):
                            for ProcessingInfo in SegmentInfo.iter(f"{pp_url}Processing"):
                                for BoardLocationInfo in ProcessingInfo.iter(f"{pp_url}BoardLocation"):
                                    for ActionInfo in BoardLocationInfo.iter(f"{pp_url}Action"):
                                        for PickInfo in ActionInfo.iter(f"{pp_url}Pick"):
                                            sREFDES = PickInfo.attrib.get('refDes')
                                            sRobotNumber = PickInfo.attrib.get('robotNumber')
                                            sHeadNumber = PickInfo.attrib.get('headNumber')

                                            for designator in designatorList:
                                                if partIs == 'NO PLACE' and sREFDES == designator:
                                                    # Handle part removal, store action info to be deleted in actionToRemove
                                                    log.debug(f"Storing {sREFDES} action with {PickInfo} in actionToRemove...")
                                                    actionToRemove.add(ActionInfo)

                                                    # Handle part removal, store robot and head to be deleted in robotHeadToRemove
                                                    robotHeadToRemove.add((sRobotNumber, sHeadNumber))
                                                    log.debug(f"Set of (RobotNumber, HeadNumber) to be removed = {robotHeadToRemove}.")

                                            # Store feeder for non-impacted designator into feeder_whitelist
                                            for non_impacted_designator in check_designator:
                                                if sREFDES == non_impacted_designator:
                                                    log.debug(f"Storing {sREFDES} Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} into feeder_whitelist...")
                                                    feeder_whitelist.add((sSectionNumber, sFeederNumber, sLaneNumber))

                                    if len(robotHeadToRemove) > 0:
                                        for ActionInfo in BoardLocationInfo.iter(f"{pp_url}Action"):
                                            for AlignInfo in ActionInfo.iter(f"{pp_url}Align"):
                                                for robotHead in robotHeadToRemove:
                                                    if AlignInfo.attrib.get('robotNumber') == robotHead[0] and AlignInfo.attrib.get('headNumber') == robotHead[1]:
                                                        # Handle part removal, store action info to be deleted in actionToRemove
                                                        log.debug(f"Storing action with {AlignInfo} in actionToRemove...")
                                                        actionToRemove.add(ActionInfo)

                                            for PlaceInfo in ActionInfo.iter(f"{pp_url}Place"):
                                                for robotHead in robotHeadToRemove:
                                                    if PlaceInfo.attrib.get('robotNumber') == robotHead[0] and PlaceInfo.attrib.get('headNumber') == robotHead[1]:
                                                        # Handle part removal, store action info to be deleted in actionToRemove
                                                        log.debug(f"Storing action with {PlaceInfo} in actionToRemove...")
                                                        actionToRemove.add(ActionInfo)

                                    # Handle part removal, delete items in actionToRemove
                                    for item in actionToRemove:
                                        log.info(f"Deleting {item} in actionToRemove...")
                                        BoardLocationInfo.remove(item)
                                    actionToRemove = set()

                            for SetupInfo in SegmentInfo.iter(f"{pp_url}Setup"):
                                for FeedSectionInfo in SetupInfo.iter(f"{pp_url}FeedSection"):
                                    sSectionNumber = FeedSectionInfo.attrib.get('number')
                                    for FeederInfo in FeedSectionInfo.iter(f"{pp_url}Feeder"):
                                        sFeederNumber = FeederInfo.attrib.get('slotNumber')
                                        for LaneInfo in FeederInfo.iter(f"{pp_url}FeederLane"):
                                            sLaneNumber = LaneInfo.attrib.get('number')
                                            sPartNumber = LaneInfo.attrib.get('partNumber')

                                            # Handle part removal, store all feeder info in feederToRemove
                                            if partIs == 'NO PLACE' and sPartNumber == partWas:
                                                if len(check_designator) <= 0:
                                                    log.info('All designators included, good to delete the feeder.')
                                                    log.debug(f"Storing {sPartNumber} {FeederInfo} to be deleted in feederToRemove...")
                                                    feederToRemove.add(FeederInfo)
                                                else:
                                                    log.info('Not all designators are included, checking if the feeder is used on any non-impacted designator before deleting...')
                                                    # Skip if the feeder is used on any non-impacted designator
                                                    if (sSectionNumber, sFeederNumber, sLaneNumber) in feeder_whitelist:
                                                        log.debug(f"Skipping {sPartNumber}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} to be deleted.")
                                                    else:
                                                        log.debug(f"Feeder only used in impacted_designator, storing {sPartNumber}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} {FeederInfo} to be deleted in feederToRemove...")
                                                        feederToRemove.add(FeederInfo)

                                            # Handle part sub, modify the feeder lane part number
                                            elif sPartNumber == partWas:
                                                if len(check_designator) <= 0:
                                                    log.info('All designators included, good to modify the feeder.')
                                                    log.info(f"Modifying {FeederInfo} from {partWas} to {partIs} ...")
                                                    LaneInfo.attrib['partNumber'] = LaneInfo.attrib['partNumber'].replace(partWas, partIs)
                                                else:
                                                    log.info('Not all designators are included, checking if the feeder is used on any non-impacted designator before modifying...')
                                                    # Skip if the feeder is used on any non-impacted designator
                                                    if (sSectionNumber, sFeederNumber, sLaneNumber) in feeder_whitelist:
                                                        raise AssertionError(f"Same feeder is sharing for impacted and non-impacted designators, unable to modify feeder, force skipping CBID = {df_input.loc[i, 'CBID']} !")
                                                    else:
                                                        log.info(f"Modifying {FeederInfo} from {partWas} to {partIs} ...")
                                                        LaneInfo.attrib['partNumber'] = LaneInfo.attrib['partNumber'].replace(partWas, partIs)

                                    # Handle part removal, delete all feeder in feederToRemove
                                    for item in feederToRemove:
                                        log.info(f"Deleting {item} in feederToRemove...")
                                        FeedSectionInfo.remove(item)
                                    feederToRemove = set()


                    # Handle .pp file
                    else:
                        pp_url = '{http://api.assembleon.com/pp/v2}'
                        log.debug(f"Setting pp_url to {pp_url}...")

                        feeder_whitelist, check_designator, all_designator, componentToRemove, feederToRemove, actionToRemove = set(), set(), set(), set(), set(), set()
                        for BoardInfo in root.iter(f"{pp_url}Board"):

                            # Modify program names for the 1st time processing the file only
                            if j == 0:
                                sPROGRAM_NAME = BoardInfo.attrib.get('id')
                                sPROGRAM_NAME_NEW = f"{sPROGRAM_NAME}-{df_input.loc[i, 'CBID']}"
                                log.info(f"Modifying program name from {sPROGRAM_NAME} to {sPROGRAM_NAME_NEW} ...")
                                BoardInfo.attrib['id'] = BoardInfo.attrib['id'].replace(sPROGRAM_NAME, sPROGRAM_NAME_NEW)

                            for ComponentInfo in BoardInfo.iter(f"{pp_url}Component"):
                                sPartNumber = ComponentInfo.attrib.get('partNumber')
                                sREFDES = ComponentInfo.attrib.get('refDes')
                                if sPartNumber == partWas and 'ALL' in designatorList: 
                                    log.debug(f"Adding {sREFDES} into all_designator...")
                                    all_designator.add(sREFDES)

                                elif sPartNumber == partWas and 'ALL' not in designatorList:
                                    log.debug(f"Adding {sREFDES} into check_designator...")
                                    check_designator.add(sREFDES)
                                
                                for designator in designatorList:

                                    # Handle part removal, store all component info of the given component PN & REFDES to be deleted
                                    if partIs == 'NO PLACE' and sPartNumber == partWas and (sREFDES == designator or designator == 'ALL'):
                                        log.debug(f"Storing {sREFDES} {ComponentInfo} to be deleted in componentToRemove...")
                                        componentToRemove.add(ComponentInfo)

                                    # Handle part sub, modify the component part number of the given REFDES
                                    elif sPartNumber == partWas and (sREFDES == designator or designator == 'ALL'):
                                        log.info(f"Modifying {sREFDES} componenet part number from {partWas} to {partIs} ...")
                                        ComponentInfo.attrib['partNumber'] = ComponentInfo.attrib['partNumber'].replace(partWas, partIs)

                            # Handle part removal, delete all component in componentToRemove
                            for item in componentToRemove:
                                log.info(f"Deleting {item} in componentToRemove...")
                                BoardInfo.remove(item)
                            componentToRemove = set()

                        if len(all_designator) > 0:
                            log.info(f"Replacing designatorList with all_designator = {all_designator} ...")
                            designatorList = all_designator

                        elif len(check_designator) > 0:
                            log.info('Checking if all designators are included...')
                            for designator in designatorList:
                                if designator != 'ALL':
                                    try:
                                        check_designator.remove(designator)
                                    except:
                                        pass
                            if len(check_designator) > 0:
                                log.info(f"Not all designators are included, non-impacted designator = {check_designator}")
                            else:
                                log.info('All designators are included.')

                        # Each section has 4 robots, total 5 sections with 20 robots, each with 1 head
                        sHeadNumber = '1'
                        robots_per_section = 4
                        section_number = 1
                        for a, ActionInfo in enumerate(root.iter(f"{pp_url}Actions")):
                            sSectionNumber = str(int(section_number))
                            if (a+1) % robots_per_section == 0:
                                section_number += 1
                            for IndexInfo in ActionInfo.iter(f"{pp_url}Index"):
                                # Enumerate to find the group from pick to place (Pick, Align, ReadFiducial, Place)
                                IndexInfoList = list(IndexInfo)
                                for k, IndexItem in enumerate(IndexInfoList):

                                    # Start whenever pick tag is found
                                    if IndexItem.tag == f"{pp_url}Pick":
                                        sREFDES = IndexItem.attrib.get('refDes')
                                        sFeederNumber = IndexItem.attrib.get('feederNumber')
                                        sLaneNumber = IndexItem.attrib.get('laneNumber')

                                        for designator in designatorList:
                                            # Handle part removal, store pick info in actionToRemove
                                            if partIs == 'NO PLACE' and sREFDES == designator:
                                                log.debug(f"Storing {sREFDES} {IndexItem} to be deleted in actionToRemove...")
                                                actionToRemove.add(IndexItem)

                                                # Handle part removal, store all subsequent tag after pick to place in actionToRemove
                                                y = k+1
                                                while y<len(IndexInfoList):
                                                    log.debug(f"Storing {sREFDES} {IndexInfoList[y]} to be deleted in actionToRemove...")
                                                    actionToRemove.add(IndexInfoList[y])

                                                    # Break if place tag is found
                                                    if IndexInfoList[y].tag == f"{pp_url}Place":
                                                        break
                                                    
                                                    y+=1
                                        
                                        # Store feeder for non-impacted designator into feeder_whitelist
                                        for non_impacted_designator in check_designator:
                                            if sREFDES == non_impacted_designator:
                                                log.debug(f"Storing {sREFDES}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} into feeder_whitelist...")
                                                feeder_whitelist.add((sSectionNumber, sFeederNumber, sLaneNumber))

                                # Handle part removal, delete items in actionToRemove
                                for item in actionToRemove:
                                    try:
                                        log.info(f"Deleting {item} in actionToRemove...")
                                        IndexInfo.remove(item)
                                    except:
                                        # Skip item with no speciied info in it
                                        pass
                                actionToRemove = set()

                        for SectionInfo in root.iter(f"{pp_url}Section"):
                            sSectionNumber = SectionInfo.attrib.get('number')
                            for TrolleyInfo in SectionInfo.iter(f"{pp_url}Trolley"):
                                for FeederInfo in TrolleyInfo.iter(f"{pp_url}Feeder"):
                                    sFeederNumber = FeederInfo.attrib.get('number')
                                    for LaneInfo in FeederInfo.iter(f"{pp_url}Lane"):
                                        sLaneNumber = LaneInfo.attrib.get('number')
                                        sPartNumber = LaneInfo.attrib.get('partNumber')

                                        # Handle part removal, store all feeder info in feederToRemove
                                        if partIs == 'NO PLACE' and sPartNumber == partWas:
                                            if len(check_designator) <= 0:
                                                log.info('All designators included, good to delete the feeder.')
                                                log.debug(f"Storing {sPartNumber} {FeederInfo} to be deleted in feederToRemove...")
                                                feederToRemove.add(FeederInfo)
                                            else:
                                                log.info('Not all designators are included, checking if the feeder is used on any non-impacted designator before deleting...')
                                                # Skip if the feeder is used on any non-impacted designator
                                                if (sSectionNumber, sFeederNumber, sLaneNumber) in feeder_whitelist:
                                                    log.debug(f"Skipping {sPartNumber}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} to be deleted.")
                                                else:
                                                    log.debug(f"Feeder only used in impacted_designator, storing {sPartNumber}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} {FeederInfo} to be deleted in feederToRemove...")
                                                    feederToRemove.add(FeederInfo)

                                        # Handle part sub, modify the feeder lane part number
                                        elif sPartNumber == partWas:
                                            if len(check_designator) <= 0:
                                                log.info('All designators included, good to modify the feeder.')
                                                log.info(f"Modifying {FeederInfo} from {partWas} to {partIs} ...")
                                                LaneInfo.attrib['partNumber'] = LaneInfo.attrib['partNumber'].replace(partWas, partIs)
                                            else:
                                                log.info('Not all designators are included, checking if the feeder is used on any non-impacted designator before modifying...')
                                                # Skip if the feeder is used on any non-impacted designator
                                                if (sSectionNumber, sFeederNumber, sLaneNumber) in feeder_whitelist:
                                                    raise AssertionError(f"Same feeder is sharing for impacted and non-impacted designators, unable to modify feeder, force skipping CBID = {df_input.loc[i, 'CBID']} !")
                                                else:
                                                    log.info(f"Modifying {FeederInfo} from {partWas} to {partIs} ...")
                                                    LaneInfo.attrib['partNumber'] = LaneInfo.attrib['partNumber'].replace(partWas, partIs)

                                # Handle part removal, delete all feeder in feederToRemove
                                for item in feederToRemove:
                                    log.info(f"Deleting {item} in feederToRemove...")
                                    TrolleyInfo.remove(item)
                                feederToRemove = set()


                # Write to a new file
                output_folder = f"{path_main}\\recipe-swr\\{df_input.loc[i, 'CBID']}"
                if not os.path.exists(output_folder):
                    log.info(f"Making folder = {output_folder} ...")
                    os.makedirs(output_folder)
                output_path = f"{output_folder}\\{filename_without_ext}-{df_input.loc[i, 'CBID']}.{file_ext}"
                log.info(f"Writing into {output_path}...")
                with open(f"{output_path}", 'wb') as f:
                    prstree.write(f)

        except AssertionError as e:
            log.warning(f"{str(e)}")
            output_folder = f"{path_main}\\recipe-swr\\{df_input.loc[i, 'CBID']}"
            if os.path.exists(output_folder):
                log.warning(f"Found output folder with warning CBID, deleting {output_folder} ...")
                delete_file(output_folder)
            continue

    log.info('Successfully completed without any errors!!!')
    log.info('Closing application...')
    time.sleep(5)

    return


if __name__ == '__main__':
    try:
        log, path_main, path_recipe_bom, path_recipe_swr, path_swr = init()
        main(log, path_main, path_recipe_bom, path_recipe_swr, path_swr)

    except ConnectionAbortedError as e:
        log.error(f"{str(e)}")
        time.sleep(5)
        sys.exit(0)

    except Exception as e:
        log.critical('Force exiting application...')
        log.exception(f"Unexpected Error: {str(e)}")
        time.sleep(5)