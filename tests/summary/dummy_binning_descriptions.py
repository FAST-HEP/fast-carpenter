bins_met_px = {"in": "MET_px", "out": "met_px", "bins": dict(nbins=10, low=0, high=100)}


bins_py = {"in": "Jet_Py", "out": "py_leadJet", "bins": dict(edges=[0, 20., 100.], overflow=True), "index": 0}


bins_nmuon = {"in": "NMuon", "out": "nmuon"}


weight_list = ["EventWeight"]


weight_dict = dict(weighted="EventWeight")
