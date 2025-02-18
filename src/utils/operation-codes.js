// src/utils/operation-codes.js
const OPERATION_CODES = {
    0: 'MACHINE OFF',
    1: 'TROUBLE MACHINE',
    2: 'CHOKOTEI',
    3: 'DANDORI',
    4: 'STOP PLANNING',
    5: 'TOOL CHANGES',
    7: 'WAITING MATERIAL',
    8: 'CONTROL LOSS TIME',
    9: 'UNKNOWN LOSS TIME',
    10: 'NORMAL OPERATION',
    11: 'TENKEN',
    13: 'TENKEN',
    14: 'NOT CONNECTED',
    21: 'JAM ISTIRAHAT',
    22: 'RENCANA PERBAIKAN',
    23: 'TRIAL',
    24: 'PLAN PROSES SELESAI',
    25: '5S',
    26: 'MEETING PAGI/SORE',
    27: 'TENKEN',
    28: 'PEMANASAN',
    29: 'CEK QC',
    30: 'INPUT DATA',
    31: 'BUANG KIRIKO',
    32: 'MENUNGGU INTRUKSI ATASAN',
    33: 'REPAIR',
    34: 'KAIZEN',
    35: 'GANTI TOISHI',
    36: 'GANTI DRESSER',
    37: '1 TOOTH',
    38: 'CHECK HAGATA',
    39: 'DRESSING PROFILE',
    40: 'DRESS-2',
    41: 'ANTRI JOB'
  };
  
  class OperationTranslator {
    static getOperationName(code) {
      return OPERATION_CODES[code] || 'UNKNOWN STATUS';
    }
  }
  
  module.exports = { OPERATION_CODES, OperationTranslator };
  