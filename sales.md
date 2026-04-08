flowchart TD

%% ------------------ START ------------------
A[Sales Engine] --> B[Lead Input]
B --> C[Set Timezone and Language]
C --> D[Default Hinglish]
D --> E[Auto Language Switch]
E --> F[Start Call and WhatsApp Together]

%% ------------------ CHANNEL SPLIT ------------------
F --> G1[Call Active 9AM to 9PM]
F --> G2[WhatsApp Active 24x7]

%% ------------------ CALL FLOW ------------------
G1 --> H{Call Result}

H -->|No Answer| H1[Retry after 2hr → 6:30PM → Next Day]
H -->|Busy| H2[Retry Next Slot]
H -->|Wrong Number| H3[Stop]
H -->|Connected| H4[Conversation]

%% ------------------ WHATSAPP FLOW ------------------
G2 --> W{User Reply}

W -->|No| W1[Follow-up Day0 Day1 Day3 Day5 Day7]
W -->|Yes| H4

%% ------------------ USER TYPE ------------------
H4 --> J{User Type}

J -->|Abusive| J1[Exit and Block]
J -->|Timepass| J2[Short Exit]
J -->|Genuine| J3[Qualification]

%% ------------------ QUALIFICATION ------------------
J3 --> K[Capture Need Budget Timeline]
K --> L{Lead Type}

%% ------------------ HOT LEAD ------------------
L -->|Hot| P[Priority Handling]

P --> P1[Notify Owner Immediately]
P --> P2[Push Call or Meeting Same Day]
P --> P3[Visit or Meeting]
P --> P4[WhatsApp Day1 Day3 Day5]

P2 --> P5[Schedule Date Time]
P2 --> P6[Send Location or Link]

P4 --> P7[Reminder 1 day / Same day / 1hr]

%% ------------------ WARM LEAD ------------------
L -->|Warm| M[Follow-up Flow]

M --> M1[Call next day 11AM or 6:30PM]
M --> M2[Convert to Meeting]

M2 --> V{Visit Status}

V -->|Done| V1[Proceed]
V -->|No Show| V2[Reschedule Next Day]

%% ------------------ COLD LEAD ------------------
L -->|Cold| C1[Campaign Flow]
C1 --> C2[Weekly Campaign Broadcast]

%% ------------------ ESCALATION ------------------
V1 --> E1{Need Escalation}

E1 -->|Yes| E2[Senior AI Call Male Voice]
E1 -->|Critical| E3[Human Intervention]

E2 --> O[Outcome]
E3 --> O

%% ------------------ POST OUTCOME ------------------
O --> O1[Notify Owner]
O1 --> R[Ask Rating 1 to 10]

%% ------------------ SCORING ------------------
R --> S{Score}

S -->|1-4| N1[Negative]
S -->|5-7| N2[Neutral]
S -->|8-10| N3[Positive]

%% ------------------ NEGATIVE ------------------
N1 --> N4[AI Resolve First]
N4 --> N5{Resolved?}

N5 -->|No| N6[Owner Alert]
N5 -->|Yes| U[Update Dashboard]

%% ------------------ NEUTRAL ------------------
N2 --> N7[Soft Referral]
N7 --> U

%% ------------------ POSITIVE ------------------
N3 --> N8[Ask Referral]
N8 --> U

%% ------------------ REPORTING ------------------
U --> R1[Morning Plan Today]
U --> R2[Evening Report Today]
U --> R3[Learning Loop]

R3 --> Z[Sales System]