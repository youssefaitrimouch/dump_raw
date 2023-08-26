
data = [
    {
        "flux": "MSS",
        "techno": "Ericsson",
        "chemin": "Ericsson",
        "table": "ericsson_tbl",
    },
    {
        "flux": "MSS",
        "techno": "Huawei",
        "chemin": "MSC_Huawei",
        "table": "huawei_tbl",
    },
    {
        "flux": "MSS",
        "techno": "Nokia",
        "chemin": "NSN",
        "table": "nokia_tbl",
    },
    {
        "flux": "OCS",
        "techno": "REC",
        "chemin": "OCSREC",
        "table": "ocsrec_tbl",
    },
    {
        "flux": "OCS",
        "techno": "VOU",
        "chemin": "OCSVOU",
        "table": "ocsvou_tbl",
    },
    {
        "flux": "OCS",
        "techno": "SMS",
        "chemin": "OCSSMS",
        "table": "ocssms_tbl",
    },
    {
        "flux": "DATA Huawei",
        "techno": "PGW",
        "chemin": "PGW3",
        "table": "pgw_tbl",
    },
    {
        "flux": "DATA Huawei",
        "techno": "SGW",
        "chemin": "4G_SGW",
        "table": "sgw_tbl",
    },
    {
        "flux": "DATA Ericsson",
        "techno": "PGW",
        "chemin": "PGW_Ericsson",
        "table": "epgw_tbl",
    },
    {
        "flux": "DATA Ericsson",
        "techno": "SGW",
        "chemin": "SGW_Ericsson",
        "table": "esgw_tbl",
    },
    {
        "flux": "DATA Ericsson",
        "techno": "SGSN",
        "chemin": "SGSN_Ericsson",
        "table": "rsgsn_tbl",
    },
    {
        "flux": "FIXE",
        "techno": "Alcatel",
        "chemin": "Alcatel",
        "table": "alcatel_tbl",
    },
    {
        "flux": "FIXE",
        "techno": "IMS_NOKIA",
        "chemin": "IMS",
        "table": "ims_tbl",
    },
    {
        "flux": "FIXE",
        "techno": "MMP",
        "chemin": "MMP",
        "table": "mmp_tbl",
    },
    {
        "flux": "FIXE",
        "techno": "Siemens",
        "chemin": "Siemens",
        "table": "siemens_tbl",
    },
    {
        "flux": "FIXE",
        "techno": "TSSA",
        "chemin": "TSSA",
        "table": "tssa_tbl",
    },
    {
        "flux": "FIXE",
        "techno": "TSSI",
        "chemin": "TSSI",
        "table": "fixe_tssi",
    }
]

# this is a class that extends the dict class, and allows you to access a dictionary like an object
# now you can do d.key instead of d["key"]
class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__