```mermaid
flowchart LR
%% Auto-generated from invoke_chain_group_003.csv
classDef integ fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20;
classDef adapt fill:#e3f2fd,stroke:#1565c0,color:#0d47a1;
 TSGT_OUTL_WACS_FPRC_PR_SYNC["TSGT Oracle Utilities WACS FPRC Pur Requisition Sy"]:::integ
 TSGT_UTIL_WACS_FPRC_APPR_REQ["TSGT Oracle Utilities WACS FPRC Approve Pur Req"]:::integ
 TSGT_OUTL_WACS_FPRC_PR_SYNC -->|ApprovePurchasRequisition| TSGT_UTIL_WACS_FPRC_APPR_REQ
```