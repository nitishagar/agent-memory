CREATE TABLE session(id TEXT PRIMARY KEY, project_id TEXT, directory TEXT, title TEXT, task_type TEXT, time_created INTEGER, time_updated INTEGER);
CREATE TABLE message(id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, time_updated INTEGER, sequence INTEGER, data TEXT);
CREATE TABLE part(id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, sequence INTEGER, data TEXT);
INSERT INTO session VALUES('sess_1111','proj_home-u-demo','/home/u/demo','demo','interactive',1785000000000,1785000060000);
INSERT INTO message VALUES('msg_1','sess_1111',1785000001000,1785000001000,0,'{"role":"user","agent":"zcode-agent","model":{"providerID":"builtin","modelID":"GLM-5.2"},"contextSnapshot":{},"semantics":{"origin":"ui"}}');
INSERT INTO message VALUES('msg_2','sess_1111',1785000002000,1785000003000,1,'{"role":"assistant","parentID":"msg_1","modelID":"GLM-5.2","providerID":"builtin","reasoningLevel":"high"}');
INSERT INTO part VALUES('prt_1','msg_1','sess_1111',1785000001000,1785000001000,0,'{"type":"text","text":"hello","time":{"start":1785000001000,"end":1785000001000}}');
INSERT INTO part VALUES('prt_2','msg_2','sess_1111',1785000002000,1785000002000,0,'{"type":"reasoning","text":"thinking aloud","time":{"start":1785000002000,"end":1785000002000}}');
INSERT INTO part VALUES('prt_3','msg_2','sess_1111',1785000002500,1785000002500,1,'{"type":"text","text":"hi there"}');
