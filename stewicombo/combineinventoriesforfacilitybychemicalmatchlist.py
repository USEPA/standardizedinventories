#Create .
#inventories_of_interest = ['DMR','TRI','NEI']

#get chemicals of interest
chemicalmatchlistfile = '../Federal-LCA-Commons-Elementary-Flow-List/chemicalmatcher/output/examplesynonymnlistfromCASlist.csv'
chemicalmatchlist = pd.read_csv(chemicalmatchlistfile,header=0)
#Have to convert TRI names to upper case
def convert_to_upper(x):
    x = str(x)
    x = str.upper(x)
    return x
chemicalmatchlist['TRI'] = [convert_to_upper(x) for x in chemicalmatchlist['TRI']]

#create a list of chemicals of interest for each program
dmr_chemicals_of_interest = list(chemicalmatchlist["DMR"])
tri_chemicals_of_interest = list(chemicalmatchlist["TRI"])
#tri_upper_case_chemicals_of_interest = []
#for chemname in tri_chemicals_of_interest:
   # chemname = str(chemname)
    #tri_upper_case_chemicals_of_interest.append(str.upper(chemname))
nei_chemicals_of_interest = list(chemicalmatchlist["NEI"])

#read in inventory
import stewi as stewi
import stewi.globals as globals
data_dir = globals.data_dir
output_dir = globals.output_dir
year = 2011

DMR = stewi.getInventory('dmr',year)
NEI = stewi.getInventory('NEI',year)
TRI = stewi.getInventory('TRI',year)

#requiredflowbyfacilityfields = ['FacilityID','FlowName','FlowAmount']
#defaultinventorycompartments = {'NEI':"air",'DMR':"water"}

#def removeuncommoninventoryfields(flowbyfacilityinventory):
#    inventory_cols = flowbyfacilityinventory.columns
#
#    if 'Compartment' not in inventory_cols:
#        #get compartment

#See unique chems
#nei_flows = pd.unique(NEI['FlowName'])

DMR_w_chemicals_of_interest = DMR[DMR["FlowName"].isin(dmr_chemicals_of_interest)]
TRI_w_chemicals_of_interest = TRI[TRI["FlowName"].isin(tri_chemicals_of_interest)]
NEI_w_chemicals_of_interest = NEI[NEI["FlowName"].isin(nei_chemicals_of_interest)]

#temp fix - make sure FacilityID is a string
NEI_w_chemicals_of_interest["FacilityID"] = NEI_w_chemicals_of_interest["FacilityID"].astype("str")
TRI_w_chemicals_of_interest["FacilityID"] = TRI_w_chemicals_of_interest["FacilityID"].astype("str")

##Import facility matches
#Try working with the long format
#Read in facility match table in long format
facilitymatchingfilelong = '../facilitymatcher/facilitymatcher/output/examplelongformatmatchinglist.csv'
facilitymatchingtable = pd.read_csv(facilitymatchingfilelong,header=0,dtype="str")







#import chemicalbyfacilitylist

chemicalbyfacilitylist = pd.read_csv('stewicombo/data/examplechemicalbyfacilitylist.csv',header=0,dtype={'CAS':"str",'FRS':"str"})

facilities_of_interest = list(chemicalbyfacilitylist["FRS"])

#merge in facility id list
master = pd.merge(chemicalbyfacilitylist,facilitymatchingtable,left_on='FRS',right_on='REGISTRY_ID',how='left')

#merge in chem match list
master = pd.merge(master,chemicalmatchlist,on="CAS",how="left")

#get emissions one at a time
#nei
chem_facility_nei = master[master["PGM_SYS_ACRNM"]=="EIS"]
#trim NEI for facilities of interest
nei_facilities_of_interest = list(chem_facility_nei["PGM_SYS_ID"])
NEI_w_chemicals_and_facilities_of_interest = NEI_w_chemicals_of_interest[NEI_w_chemicals_of_interest["FacilityID"].isin(nei_facilities_of_interest)]
chem_facility_nei_withchem = pd.merge(chem_facility_nei,NEI_w_chemicals_and_facilities_of_interest,left_on=['PGM_SYS_ID','NEI'],right_on=['FacilityID','FlowName'])
chem_facility_nei_withchem = chem_facility_nei_withchem[['CAS','FRS','NEI','FacilityID','FlowAmount']]
chem_facility_nei_withchem.rename(columns={'FRS':'FRS_ID','NEI':'NEI_Chemical_Name','FacilityID':'NEI_ID','FlowAmount':'NEI_Amount_kg'},inplace=True)


#tri
chem_facility_tri = master[master["PGM_SYS_ACRNM"]=="TRIS"]
#trim NEI for facilities of interest
tri_facilities_of_interest = list(chem_facility_tri["PGM_SYS_ID"])
TRI_w_chemicals_and_facilities_of_interest = TRI_w_chemicals_of_interest[TRI_w_chemicals_of_interest["FacilityID"].isin(tri_facilities_of_interest)]
chem_facility_tri_withchem = pd.merge(chem_facility_tri,TRI_w_chemicals_and_facilities_of_interest,left_on=['PGM_SYS_ID','TRI'],right_on=['FacilityID','FlowName'])
chem_facility_tri_withchem = chem_facility_tri_withchem[['CAS','FRS','TRI','Compartment','FacilityID','FlowAmount']]
chem_facility_tri_withchem.rename(columns={'FRS':'FRS_ID','TRI':'TRI_Chemical_Name','FacilityID':'TRI_ID','FlowAmount':'TRI_Amount_kg','Compartment':'TRI_Compartment'},inplace=True)
#Needs to be unstacked to diff emissions

#export pieces
chem_facility_nei_withchem.to_csv('neireleases_forVinit_2May2018.csv',index=False)
chem_facility_tri_withchem.to_csv('trireleases_forVinit_2May2018.csv',index=False)

#merge files
#final = pd.merge(chem_facility_nei_withchem,chem_facility_tri_withchem,on=['CAS','FRS_ID'])
#final.to_csv('releaselist_forVinit_2May2018.csv',index=False)

