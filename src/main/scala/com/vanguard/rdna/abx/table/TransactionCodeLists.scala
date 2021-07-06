package com.vanguard.rdna.abx.table

case class TransactionCodeLists(recurringCodes: List[String], nonRecurringCodes: List[String], invalidCodes: List[String] = List.empty) {
  val validCodes: List[String] = recurringCodes ::: nonRecurringCodes
  val allCodes: List[String] = validCodes ::: invalidCodes
}

object TransactionCodeLists {
  val bdCodeLists = TransactionCodeLists(
    recurringCodes = List("DDP", "DDPR", "MMDD"),
    nonRecurringCodes = List("ACH", "ADP", "BKD", "BKR", "CAT", "CCO", "CHK", "CNV1", "CNV2", "CONTC", "CONTP", "COP", "CPO",
      "CPU", "CPY", "CRA", "CRC", "CRD", "CSR", "DBEN", "DDRO", "DDRO1", "DEA", "DEP", "DIRI", "DIRQ", "DRDTH", "DRI", "DRQ",
      "DRRO", "DRS", "DTRF", "DWR", "EARNC", "EARNP", "EDCC", "EDU1", "EMPLC", "EMPLP", "EMPRC", "EMPRP", "ENX", "EPX", "ERE8",
      "ERE8E", "EREP", "EREPE", "EXCE", "EXCE2", "EXCES", "EXCP", "EXCPC", "EXCPP", "EXCPR", "EXCR", "EXCRE", "EXPPR", "EXRD",
      "EXRP", "EXRPE", "FEDWT", "FGNWT", "FPUT", "HAF", "IRACC", "IRACP", "JRL", "LDA", "LNO", "LPR", "LRE", "LTS", "MACH", "MCA",
      "MCK", "MDR", "NOR", "PCV", "PRE", "PREM", "PREMX", "PTF", "RAT", "RCC1", "RCC2", "RCP1", "RCP2", "RCRT", "RCTR", "RCVN",
      "RCZ1", "RCZ2", "RDCT", "RDK", "RDPT", "RDT", "RECHK", "REG", "REW", "RFF", "RFW", "RHE1", "RHE2", "RMR", "ROLI", "ROLL",
      "ROLX", "RPR2", "RPRM", "RREG", "RTDP", "RTH1", "RTH2", "RTTL", "SES", "SFW", "T/A", "TBA", "TCA", "TOTL", "TOTP", "TPF",
      "TPRE", "TRC", "TRI", "TRU", "TSI", "WBEN", "WDRO", "WDRO1", "WRI", "WTRF"),
    invalidCodes = List(
      "", "IRE", "PDIRI", "CFEE", "FLRV", "/AR", "/AT", "TYP", "SFC", "ADJ1", "ADJN", "ADJT", "ADJU", "ADJY", "ADJV", "ADJ4", "ADJ7",
      "ADJC", "AFRC", "ADJD", "ADJI", "ADJL", "AOIS", "ADJQ", "ADJS", "ADJP", "ADJE", "ADJR", "ADJZ", "ADJB", "ASPN", "TAA", "ADJ",
      "AOC", "AMD", "ADM", "RAF", "APD", "/AF", "PIA", "/IA", "PCE", "PCF", "PCM", "PRK", "PCK", "PCJ", "PCC", "PRC", "1PS", "NET",
      "ARN", "RNS", "C-D", "TO411", "CKA", "QMP", "NA", "TO950", "DK/", "SCP", "DCMG", "/IM", "FR903", "FORMU", "/PB", "FR440", "FR399",
      "TDL", "ADJW", "FR506", "DTC", "IRC", "RXP", "FR603", "TO550", "FAUTO", "FEXCS", "FR501", "DWD", "WNC", "FDRS", "RE405", "REC55", "EXCS",
      "FR510", "TO300", "YRINC", "TO460", "DRF", "/IF", "TAUTO", "DO6", "TRN", "TO903", "/GT", "FR950", "/RG", "RPS", "FRDTC", "TAP", "FR909",
      "ICW", "FREI", "SOA", "ICC", "FR411", "FR365", "PWC", "FR505", "NSCC", "TSA", "CDA", "FR300", "CRV", "ECT", "MMP", "TO502", "DAF", "MFP",
      "FAF", "STG", "FDE", "SYN", "TO399", "RXN", "FRE", "TO603", "ADST", "FR999", "TO604", "ACM", "FR604", "SOR", "PIR", "TO401", "DPE", "MDEL",
      "TO508", "TCAL", "ACI", "CCR", "SGR", "FT4", "QRC", "API", "CRS", "ARB", "NRC", "RAP", "RWC", "SRX", "Retai", "TO909", "ODV*", "NXS", "TSEG", "FR605",
      "TRS", "COM", "FR500", "TO928", "NTM", "SOS", "TO451", "TO360", "PRN", "TO251", "FR509", "FR928", "EXS", "TO365", "STW", "DTM", "FR550", "RIN", "MMB",
      "TO505", "ITS", "DEV", "TO998", "FRTFR", "MOA", "QMA", "FM460", "TREI", "SPF", "RFA", "TTX", "ERR", "MMJ", "NTT", "AEW", "/ME", "FEB", "DWA", "PFA",
      "WCO", "TO605", "NSS", "PAT", "FR701", "TO500", "FM451", "TODTC", "JRL F", "RTW", "FR508", "FSEG", "SEXCS", "QRA", "PLEG", "QMC", "P/O", "TO506", "T0701",
      "DWC", "DPA", "WOA", "TOTRF", "STAX*", "FRTRF", "FR360", "SF", "FT0", "TO509", "TO501", "FGA", "SFA", "/XM", "RDI", "TO440", "PER", "RNU", "TAF", "CREC",
      "ASPO", "AFS", "RSE", "OTF", "RELS", "AMC", "HDS", "RNT", "TFN", "ECP", "FR42", "BAI", "LRD", "LRC", "BNA", "VBA", "VBM", "INA", "MUA", "INS", "INE", "INZ",
      "MUN", "REIMB", "CHRG", "BASF", "CHRGR", "RCB", "CFW", "DCAP", "/CD", "CD/", "DIA", "DIV", "DVE", "FTND", "CTND", "FRAC", "CIL", "CIA", "CEL", "LEO", "CIL",
      "LEA", "LES", "LEU", "CSI", "CSD", "CCHK", "COB", "COC", "/ZS", "/ZO", "/RB", "/RO", "/RS", "/CC", "CFP", "CCB", "CVA", "ACF", "CFC", "CPD", "CONV",
      "PCG", "CNV", "CNA", "SDC", "IF/", "CIP", "CINTR", "CON", "RCC", "ECN", "RCR", "CCI", "RMF", "CHG", "C/C", "CAA", "CBKR", "CCAL", "TFA", "DOC", "RDD", "DEF",
      "FDA", "PDA", "DLA", "TDA", "DEL", "DEL", "FDMA", "DRD", "RNO", "DIS", "DCA", "DIST", "RLCP", "RSCP", "RDCP", "DCR", "DIV", "DIV*", "RDDV", "RDIV", "DRA",
      "DVA", "DO0", "DO2", "DO3", "DO4", "DO7", "DO5", "DO1", "/DK", "/DP", "DBD", "RAD", "ELE", "ELA", "PCD", "RC/", "EDC", "RDC", "ESA", "SOF", "XCH", "EXC",
      "EXCH", "ECA", "/XA", "/XN", "/XG", "EXE", "EXA", "EOC", "EXP", "FRPR2", "FTOTL", "FRREG", "FRPRM", "FRTTL", "FDEC2", "FDEC1", "FPREM", "FDEXR", "FDERE", "EARNF", "FDEXC",
      "FDEXP", "FREG", "FRTH2", "FCNV2", "FPREX", "FTPRE", "FRHE2", "FRTH1", "FRHE1", "FRDCT", "FRDPT", "FTOTP", "FCNV1", "FEDWR", "FWA", "FFA", "SFF", "FTW", "FWT",
      "FT7", "FT1", "FTX", "FGF", "FPA", "FPC", "FPP", "FBKR", "FLIQ", "FRDP", "MSC5", "INF", "FXC", "DFA", "DVS", "DVF", "/FS", "FTR", "FGN", "STAX", "F/S",
      "FSL", "FSR", "FPE", "FPS", "CSA", "IFD", "ISF", "INM", "INT", "INX", "INJ", "IFF", "INT", "RINT", "DITRF", "WITRF", "MGRFE", "RNV", "JNL", "GNL", "LGL", "/LG",
      "LPS", "/LP", "LQA", "LQS", "LQD", "LIQ", "LIA", "LIS", "LCAP", "CGA", "CGS", "CGN", "/ML", "QMF", "MCH", "MPD", "MGA", "MMF", "DIP", "/XT", "IN/",
      "INTR", "SMA", "MTM", "PCA", "MAT", "FMGR", "MEA", "CMGR", "MER", "MSC6", "MCR", "MFEE", "SWP", "MMR", "SWR", "MFS", "MMA", "MOP", "MOR", "/WI", "MFA", "FEX",
      "/FF", "MLF", "CLX", "SXCH", "MFR", "N/C", "/AC", "APN", "CDX", "DLR", "MKS", "MKE", "SPM", "CWW", "CDQ", "CAI", "CAQ", "CSP", "POP", "MUF", "NET",
      "NQI", "NQA", "EDU2", "NRD", "NRA", "NRT", "NTA", "NTS", "NTD", "RND", "NOI", "DNRA", "NRREG", "INRA", "CNS", "/NT", "SRT", "ASG", "/OC", "/XO", "/OR",
      "MSC3", "TF0", "ALTFE", "OSUB", "RPDT", "MLP", "PDST", "MLA", "MLS", "PIK", "PCS", "PRS", "STP", "PCP", "TPM", "PPP", "PSC", "PES", "TCU", "RPD",
      "PPO", "RPY", "RPC", "APX", "ARI", "ARQ", "PCI", "NOA", "NOF", "APR", "ARC", "PYR", "PPA", "PPS", "PPD", "PRIN", "/PS", "PYB", "PYD", "IFP",
      "INP", "PSR", "ERNF", "ERNS", "MCAP", "QADJ", "/IR", "FEEPD", "REA", "LPA", "CUA", "CLA", "MRA", "FRA", "RPA", "RAA", "REC", "CASH", "TSF",
      "TNL", "SEQ", "TEQ", "SFK", "TOL", "REC", "SOW", "RCT", "RAC", "ARR", "RCLM", "RDA", "RDF", "RDR", "EUR", "ECR", "RCE", "COT", "RPE", "FEERE", "RROC",
      "DVR", "RMCP", "RPRN", "RGCP", "RTCK", "RIR", "/RP", "RSK", "RCY", "RTA", "RACH", "ROC", "POR", "ROA", "ROS", "ROP", "RCA", "/RT", "PRR", "PCR", "R/S",
      "RDST", "RSA", "CRR", "RRA", "RRS", "RRM", "ROC", "RRC", "RMA", "ACT", "SRTH1", "ROP", "FEE", "GCAP", "/SI", "LSR", "BOX", "S/U", "BID", "CAL", "CVT", "DWS",
      "EXC", "EXP", "LIQ", "DMA", "DMD", "RGM", "SHP", "RFD", "CCA", "RDM", "RDP", "ROG", "TND", "SVC", "IR/", "SGA", "SGS", "SGN", "SCAP", "SFP", "PCT",
      "SPP", "SPC", "RSP", "/SR", "SPIN", "SRREG", "SRPRM", "SRPR2", "SRTTL", "SDEC2", "SDEC1", "SPREM", "SPREX", "EARNS", "STEXC", "STEXP", "SREG",
      "SRTH2", "SCNV2", "STPRE", "SRHE2", "SRHE1", "SRDPT", "SRDCT", "STOTP", "SCNV1", "SDTAX", "SMSC4", "STTAX", "STOTL", "STEXR", "STERE", "SDA",
      "SDS", "SDV", "SPA", "SPS", "SPL", "DST", "STPT", "SUB", "SFR", "SPY", "ITE", "DTAX", "ITAX", "OTAX", "TTAX", "TCE", "TNA",
      "TER", "RTF", "TPD", "T/D", "CTE", "SRR", "TAR", "TRA", "MFX", "/UT", "UCAP", "UBT", "FEE", "VBF", "VCK", "VDS", "/VR", "/TN", "WFF",
      "/WF", "MSC4", "WHT", "WOFF", "W/O", "WOF", "TOA", "ASG", "XER", "XPD", "TFR", "SVP", "COR","/MF", "MGR",
      "MGN", "UBTC", "UBTI","ADJ5", "ADJ6", "SRTTL", "SMSC4"
    )
  )

  val taCodeLists = TransactionCodeLists(
    recurringCodes = List("5003", "5011", "5651", "5652", "5653", "5654", "7004", "7005", "7006", "7048", "7651", "7652", "7653",
      "7654", "7655", "6252", "6253", "6254", "6255", "6256"),
    nonRecurringCodes = List("5001", "5002", "5004", "5005", "5006", "5007", "5008", "5009", "5012", "5013", "5014", "5015", "5018",
      "5019", "5020", "5022", "5025", "5030", "5035", "5060", "5102", "5021", "5200", "5201", "5202", "5203", "5204", "5205", "5206",
      "5207", "5208", "5209", "5210", "5220", "5222", "5225", "5231", "5232", "5233", "5234", "5235", "5236", "5302", "9590", "7001",
      "7002", "7003", "7009", "7010", "7056", "7057", "7058", "7059", "7060", "7061", "7062", "7063", "7064", "7065", "7066", "7067",
      "7068", "7069", "7070", "7076", "7077", "9564", "7201", "7202", "7203", "7204", "7205", "7206", "7207", "7210", "7211", "7228",
      "7229", "7230", "7232", "6200", "6201", "6202", "6203", "6204", "6205", "6207", "6212", "6214", "6225", "6226", "6245", "6251",
      "6300", "6304", "6400", "6404", "7209", "7213", "7225", "7251", "7701", "7702", "7703", "7704", "7705", "7706", "7707", "7708",
      "7709", "7710", "7711", "7712", "7713", "7714", "7715", "7716", "7717", "7718", "7719", "7720", "7721", "7722", "7723", "7724",
      "7725", "7726", "7802", "7803", "7804", "7805", "7812", "7813", "9501", "9521", "9704", "6208", "6209", "6213", "6215", "6216",
      "6220", "6221", "6224", "6227", "6228", "6229", "6246", "6408", "6409", "7212", "7222", "7313", "7731", "7734", "7760", "7764",
      "9550", "9551", "9552", "9553", "9555", "9556", "9557", "9558", "9559", "9701", "9702", "9705", "9706", "9709", "9710", "5664",
      "5091", "5092", "5096", "5098", "5099", "4001", "4003", "4005", "4006", "4007", "4101", "4102", "4105", "4106", "4107", "4108",
      "4121", "4122", "4125", "4126", "4127", "4128", "4002", "4004", "4008", "4012", "4104", "4124"),

    invalidCodes = List(
      "4009", "4010", "4109", "4110", "4115", "4116", "4117", "4118", "4129", "4130", "8213", "8300", "5010", "5016", "5017", "5023",
      "5024", "5061", "5063", "7047", "8026", "9562", "9571", "9572", "9573", "9578", "9592", "9601", "9603", "9611", "9694", "9696",
      "5026", "5300", "5301", "5655", "6257", "8202", "8212", "9201", "9222", "9223", "9232", "9233", "9242", "9243", "9252", "9253",
      "9574", "9575", "9600", "9602", "9610", "9675", "9690", "9693", "9695", "9697", "8010", "8012", "8013", "8014", "8015", "8016",
      "8017", "8018", "8019", "8020", "8022", "8023", "8024", "8025", "8040", "8043", "8111", "8112", "8113", "8126", "8127", "8128",
      "9505", "9506", "9540", "9541", "9561", "9563", "9567", "9570", "9591", "9593", "9594", "9800", "9801", "9804", "9811", "9820",
      "9821", "8021", "8029", "8030", "8032", "8033", "8035", "8039", "8041", "9542", "9566", "9569", "9595", "9802", "9822", "7049",
      "7208", "7214", "7221", "9211", "9674", "6206", "6210", "6211", "7215", "7217", "7218", "7219", "7226", "7227", "9202", "9203",
      "9204", "9205", "9212", "9213", "9214", "9215", "6217", "6218", "6230", "6234", "6240", "6244", "6260", "6264", "7216", "7220",
      "7223", "7224", "7317", "7318", "7730", "7740", "7744", "7750", "7754", "8224", "7301", "7302", "7303", "7304", "7305", "7306",
      "7311", "7312", "8101", "8102", "8103", "8104", "8105", "8106", "8107", "8108", "8114", "8115", "8116", "8117", "8118", "8119",
      "8120", "8121", "8122", "8123", "8124", "8125", "8136", "8137", "8138", "8139", "8201", "8203", "8205", "8206", "8271", "8272",
      "8372", "9105", "9106", "9524", "9525", "9526", "9527", "9528", "9529", "9536", "9537", "9538", "9539", "9775", "9807", "9808",
      "9813", "9814", "9815", "9816", "9817", "9818", "9824", "9825", "9826", "9827", "9836", "9837", "9554", "8273", "9517", "8027",
      "8028", "8031", "8034", "8036", "8037", "8038", "8042", "9543", "9565", "9568", "9596", "9803", "9823", "8044", "8045", "8046",
      "8047", "8048", "8049", "9530", "9576", "9577", "9597", "9805", "6225R", "6227R", "5001R", "5002R", "5003R", "5005R", "5010R",
      "5015R", "5016R", "5017R", "5019R", "5020R", "5021R", "5022R", "5024R", "5026R", "5030R", "5035R", "5063R", "5201R", "5202R",
      "5203R", "5204R", "5205R", "5207R", "5208R", "5209R", "5210R", "5222R", "5231R", "5232R", "5233R", "5234R", "5236R", "5300R",
      "5301R", "5651R", "5653R", "6200R", "6201R", "6204R", "6205R", "6208R", "6209R", "6210R", "6211R", "6212R", "6213R", "6214R",
      "6215R", "6216R", "6220R", "6224R", "6225R", "6227R", "6225R", "6227R", "6228R", "6245R", "6246R", "6400R", "6404R", "7001R",
      "7002R", "7003R", "7004R", "7005R", "7006R", "7047R", "7048R", "7064R", "7065R", "7066R", "7069R", "7070R", "7076R", "7077R",
      "7201R", "7202R", "7203R", "7204R", "7205R", "7206R", "7207R", "7208R", "7210R", "7211R", "7213R", "7214R", "7217R", "7218R",
      "7222R", "7223R", "7224R", "7225R", "7226R", "7227R", "7228R", "7229R", "7230R", "7232R", "7313R", "7317R", "7318R", "7651R",
      "7653R", "7701R", "7702R", "7703R", "7704R", "7705R", "7706R", "7710R", "7711R", "7712R", "7719R", "7720R", "7725R", "7726R",
      "7764R", "8018R", "8019R", "8020R", "8021R", "8022R", "8023R", "8024R", "8025R", "8027R", "8028R", "8029R", "8030R", "8031R",
      "8032R", "8033R", "8034R", "8036R", "8037R", "8038R", "8039R", "8040R", "8041R", "8042R", "8043R", "8044R", "8045R", "8046R",
      "8047R", "8048R", "8111R", "8112R", "8113R", "8126R", "8127R", "8128R", "8201R", "8271R", "8272R", "8273R", "8372R", "9565R",
      "9566R", "9568R", "9569R", "9571R", "9572R", "9573R", "9574R", "9576R", "9577R", "9578R", "9591R", "9592R", "9594R", "9595R", "9596R"
    )
  )

}

