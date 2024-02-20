# SWR_PROGRAM_CREATE
To automate SWR program creation, generated based on bom programs. Developed by [jiajunlee](https://github.com/jiajunlee19).
- The current [swr-program-create](https://github.com/jiajunlee19/swr-program-create) project is for python-developer only. If you wish to continue, installation are provided in below section.
- For non-python user, please navigate to the alternative [swr-program-create-app](https://github.com/jiajunlee19/swr-program-create-app) solution.

<br>

There are generally 3 types of SWR, see limitations in below section.
1. `SWR Part Extra Place` or `Adding Part` (Low demand - ~10%)
2. `SWR Part No Place` or `Removing Part` (Low demand - ~10%)
3. `SWR Part Number/Suffix Sub` or `Subbing Part` (High demand ~80%)

![flowchart.png](Misc/flowchart.png)

<br>

### Installation
- Fork this project [here](https://github.com/jiajunlee19/swr-program-create/fork)
<br><br>
    OR
<br><br> 
- Clone this project by `git clone "https://github.com/jiajunlee19/swr-program-create.git"`
<br><br>
    OR
<br><br> 
- Download this project in zip [here](https://github.com/jiajunlee19/swr-program-create/archive/refs/heads/master.zip) and extract

<br>

### How to run?
0. (Optional) Go to `settings` sheet in [SWR.xlsx](SWR.xlsx), modify the settings if needed.
1. Go to `SWR` sheet in [SWR.xlsx](SWR.xlsx), fill in all SWR requirements accordingly.
2. Place all relevant bom recipes in [recipe-bom](recipe-bom/) folder.
3. Run [main.py](main.py).
4. Created SWR recipes can be found in [recipe-swr](recipe-swr/) folder within subfolder grouped by `CBID`.
5. (Optional): View the logs in [Log/SWR_PROGRAM_CREATE.log](Log/SWR_PROGRAM_CREATE.log).

<br>

### Limitations
Below are the limitations that cannot be handled. Warning will be raised as such the impacted CBID will be skipped.
1. `SWR Part Extra Place` or `Adding Part` cannot be handled.
2. `SWR Part Sub` or `Subbing Part` with feeder used in any non-impacted designator cannot be handled.

<br>

### Test Cases
Below are the passing test cases, using master bom recipes in [recipe-bom/590-123456/](recipe-bom/590-123456/).

| SWR Requirements                     | Generated SWR recipes                                                          |
| :---                                 | :-------------------------                                                     |
| [123456-EXTRAPLACE](SWR.xlsx)        | No SWR recipes generated as per limitation `#1`                                |
| [123457-NOPLACE-ALL](SWR.xlsx)       | [All imapcted parts and feeders are removed](recipe-swr/123457-NOPLACE-ALL/)   |
| [123458-NOPLACE-Partial](SWR.xlsx)   | [All impacted parts are removed, keeping feeders for non-impacted designators ](recipe-swr/123458-NOPLACE-PARTIAL/) |
| [123459-PARTSUB-ALL](SWR.xlsx)       | [All impated parts and feeders are subbed](recipe-swr/123459-PARTSUB-ALL/)     |
| [123460-PARTSUB-PARTIAL](SWR.xlsx)   | No SWR recipes generated as per limitation `#2`                                |

<br>

### Details
Here are some details if you are interested on how the `.pp` or `.pp7` are modified and being generated into `recipe-bom-new`.

0. SWR requirement
    <br>
    ![SWR requirements.png](Misc/0.%20SWR%20requirements.PNG)
    <br>
    ![Place BOM Recipes.png](Misc/0.%20Place%20BOM%20Recipes.PNG)

<br>

1. Part Removal - Delete Component
    <br>
    ![Part Removal - Delete Component.png](Misc/1.%20Part%20Removal%20-%20Delete%20Component.PNG)

<br>

2. Part Removal - Delete Feeder
    <br>
    ![Part Removal -Delete Feeder.png](Misc/2.%20Part%20Removal%20-%20Delete%20Feeder.PNG)

<br>

3. Part Removal - Delete Pick to Place
    * For pp
        <br>
        ![Part Removal - Delete Pick to Place.png](Misc/3.%20Part%20Removal%20-%20Delete%20Pick%20to%20Place.PNG)
        <br>
    * For pp7
        <br>
        ![Part Removal - Delete Pick to Place pp7.png](Misc/3.%20Part%20Removal%20-%20Delete%20Pick%20to%20Place%20pp7.PNG)

<br>

4. Part Sub - Modify Component
    <br>
    ![Part Sub - Modify Component.png](Misc/4.%20Part%20Sub%20-%20Modify%20Component.PNG)

<br>

5. Part Sub - Modify Feeder
    <br>
    ![Part Sub - Modify Feeder.png](Misc/5.%20Part%20Sub%20-%20Modify%20Feeder.PNG)

<br>

6. Generate SWR Recipe
    <br>
    ![Generate SWR Recipe.png](Misc/6.%20Generate%20SWR%20Recipe.PNG)

<br>