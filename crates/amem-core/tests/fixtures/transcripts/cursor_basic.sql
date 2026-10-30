CREATE TABLE cursorDiskKV(key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);
INSERT INTO cursorDiskKV VALUES
('composerData:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
 '{"_v":17,"composerId":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","unifiedMode":"agent","status":"none","text":"","fullConversationHeadersOnly":[{"bubbleId":"b1111111-1111-4111-8111-111111111111","type":1},{"bubbleId":"b2222222-2222-4222-8222-222222222222","type":2},{"bubbleId":"b3333333-3333-4333-8333-333333333333","type":2}],"conversationMap":{}}'),
('bubbleId:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:b1111111-1111-4111-8111-111111111111',
 '{"type":1,"bubbleId":"b1111111-1111-4111-8111-111111111111","text":"add retries","createdAt":"2026-10-30T00:00:00.000Z","workspaceProjectDir":"/home/u/proj"}'),
('bubbleId:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:b2222222-2222-4222-8222-222222222222',
 '{"type":2,"bubbleId":"b2222222-2222-4222-8222-222222222222","text":"Done.","createdAt":"2026-10-30T00:00:05.000Z","workspaceProjectDir":"/home/u/proj","modelInfo":{"modelName":"gpt-5.2"}}'),
('bubbleId:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:b3333333-3333-4333-8333-333333333333',
 '{"type":2,"bubbleId":"b3333333-3333-4333-8333-333333333333","text":"","createdAt":"2026-10-30T00:00:03.000Z","workspaceProjectDir":"/home/u/proj","toolFormerData":{"name":"read_file","rawArgs":"{\"path\":\"a\"}"}}');
