package whatsapp

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/config"
	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestHandleMessageReactionStoresReactionAndForwardsWebhook(t *testing.T) {
	originalWebhookURLs := config.WhatsappWebhook
	originalWebhookEvents := config.WhatsappWebhookEvents
	originalAutoReply := config.WhatsappAutoReplyMessage
	originalAutoMarkRead := config.WhatsappAutoMarkRead
	originalSubmit := submitWebhookFn
	originalLog := log
	defer func() {
		config.WhatsappWebhook = originalWebhookURLs
		config.WhatsappWebhookEvents = originalWebhookEvents
		config.WhatsappAutoReplyMessage = originalAutoReply
		config.WhatsappAutoMarkRead = originalAutoMarkRead
		submitWebhookFn = originalSubmit
		log = originalLog
	}()

	log = waLog.Noop
	config.WhatsappWebhook = []string{"https://example.test/webhook"}
	config.WhatsappWebhookEvents = nil
	config.WhatsappAutoReplyMessage = ""
	config.WhatsappAutoMarkRead = false

	repo := &messageHandlerRepoSpy{}
	done := make(chan map[string]any, 1)
	submitWebhookFn = func(_ context.Context, payload map[string]any, _ string, _ *domainChatStorage.DeviceWebhookConfig) error {
		done <- payload
		return nil
	}

	evt := reactionEventForTest("reaction-event-1", "msg-1", "\U0001f44d")
	handleMessage(context.Background(), evt, repo, nil)

	if got := repo.createReactionCount(); got != 1 {
		t.Fatalf("expected reaction path to call CreateReaction once, got %d", got)
	}
	if got := repo.createMessageCount(); got != 0 {
		t.Fatalf("expected reaction path not to call CreateMessage, got %d", got)
	}

	select {
	case payload := <-done:
		if got := payload["event"]; got != EventTypeMessageReaction {
			t.Fatalf("expected webhook event %q, got %v", EventTypeMessageReaction, got)
		}
		eventPayload, ok := payload["payload"].(map[string]any)
		if !ok {
			t.Fatalf("expected payload map, got %T", payload["payload"])
		}
		if got := eventPayload["reaction"]; got != "\U0001f44d" {
			t.Fatalf("expected reaction in webhook payload, got %v", got)
		}
		if got := eventPayload["reacted_message_id"]; got != "msg-1" {
			t.Fatalf("expected reacted message id, got %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for webhook submission")
	}
}

func TestHandleMessagePersistsPollBeforeForwardingWebhook(t *testing.T) {
	originalWebhookURLs := config.WhatsappWebhook
	originalWebhookEvents := config.WhatsappWebhookEvents
	originalSubmit := submitWebhookFn
	originalLog := log
	defer func() {
		config.WhatsappWebhook = originalWebhookURLs
		config.WhatsappWebhookEvents = originalWebhookEvents
		submitWebhookFn = originalSubmit
		log = originalLog
	}()
	log = waLog.Noop
	config.WhatsappWebhook = []string{"https://example.test/webhook"}
	config.WhatsappWebhookEvents = nil

	repo := &messageHandlerRepoSpy{}
	done := make(chan map[string]any, 1)
	submitWebhookFn = func(_ context.Context, payload map[string]any, _ string, _ *domainChatStorage.DeviceWebhookConfig) error {
		done <- payload
		return nil
	}
	ctx := ContextWithDevice(context.Background(), NewDeviceInstance("device-a", nil, nil))
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: types.NewJID("120363000000", types.GroupServer)},
			ID:            "POLL-HANDLER-1",
			Timestamp:     time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC),
		},
		Message: &waE2E.Message{PollCreationMessageV3: &waE2E.PollCreationMessage{
			Name: protoString("Lunch?"),
			Options: []*waE2E.PollCreationMessage_Option{
				{OptionName: protoString("Pizza")},
				{OptionName: protoString("Sushi")},
			},
		}},
	}

	handleMessage(ctx, evt, repo, nil)
	select {
	case delivered := <-done:
		payload := delivered["payload"].(map[string]any)
		poll, ok := payload["poll"].(*webhookPollPayload)
		if !ok || poll.Type != "creation" || len(poll.Options) != 2 || payload["body"] != "Poll: Lunch?" {
			t.Fatalf("unexpected webhook payload: %+v", payload)
		}
		repo.mu.Lock()
		definition := repo.pollDefinition
		repo.mu.Unlock()
		if definition == nil || definition.PollMessageID != "POLL-HANDLER-1" {
			t.Fatalf("poll was not persisted before webhook delivery: %+v", definition)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for poll webhook")
	}
}

func TestHandleWebhookForwardForwardsStatusAsDedicatedEvent(t *testing.T) {
	originalWebhookURLs := config.WhatsappWebhook
	originalWebhookEvents := config.WhatsappWebhookEvents
	originalChatwootEnabled := config.ChatwootEnabled
	originalSubmit := submitWebhookFn
	originalLog := log
	defer func() {
		config.WhatsappWebhook = originalWebhookURLs
		config.WhatsappWebhookEvents = originalWebhookEvents
		config.ChatwootEnabled = originalChatwootEnabled
		submitWebhookFn = originalSubmit
		log = originalLog
	}()

	log = waLog.Noop
	config.WhatsappWebhook = []string{"https://example.test/webhook"}
	config.WhatsappWebhookEvents = nil

	delivered := make(chan map[string]any, 8)
	submitWebhookFn = func(_ context.Context, payload map[string]any, _ string, _ *domainChatStorage.DeviceWebhookConfig) error {
		delivered <- payload
		return nil
	}

	// Status posts use a dedicated event so webhook consumers can persist them
	// outside the chat inbox. This remains safe with Chatwoot enabled because
	// its own routing rejects the system broadcast JID.
	statusChat := types.NewJID("status", types.BroadcastServer)
	config.ChatwootEnabled = true
	handleWebhookForward(context.Background(), textEventForTest("status-1", statusChat), nil)

	// A regular direct message keeps the generic event contract.
	config.ChatwootEnabled = false
	dmChat := types.NewJID("628123456789", types.DefaultUserServer)
	handleWebhookForward(context.Background(), textEventForTest("dm-1", dmChat), nil)

	seen := map[string]string{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 2 {
		select {
		case payload := <-delivered:
			eventPayload, ok := payload["payload"].(map[string]any)
			if !ok {
				t.Fatalf("expected payload map, got %T", payload["payload"])
			}
			seen[eventPayload["id"].(string)] = payload["event"].(string)
		case <-deadline:
			t.Fatalf("timed out waiting for webhook submissions: %+v", seen)
		}
	}
	if seen["status-1"] != EventTypeStatusMessage {
		t.Fatalf("status event = %q, want %q", seen["status-1"], EventTypeStatusMessage)
	}
	if seen["dm-1"] != EventTypeMessage {
		t.Fatalf("direct message event = %q, want %q", seen["dm-1"], EventTypeMessage)
	}
}

func textEventForTest(eventID string, chat types.JID) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     chat,
				Sender:   types.NewJID("628111111111", types.DefaultUserServer),
				IsFromMe: false,
			},
			ID:        eventID,
			Timestamp: time.Date(2026, time.May, 16, 8, 0, 0, 0, time.UTC),
		},
		Message: &waE2E.Message{
			Conversation: protoString("hello"),
		},
	}
}

type messageHandlerRepoSpy struct {
	domainChatStorage.IChatStorageRepository
	mu                  sync.Mutex
	createMessageCalls  int
	createReactionCalls int
	pollDefinition      *domainChatStorage.PollDefinition
}

func (r *messageHandlerRepoSpy) UpsertPollDefinition(definition *domainChatStorage.PollDefinition) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pollDefinition = definition
	return nil
}

func (r *messageHandlerRepoSpy) GetPollDefinition(_, _, _ string) (*domainChatStorage.PollDefinition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pollDefinition, nil
}

func (r *messageHandlerRepoSpy) AppendPollOption(_, _, _ string, option domainChatStorage.PollOption) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pollDefinition != nil {
		r.pollDefinition.Options = append(r.pollDefinition.Options, option)
	}
	return nil
}

func (r *messageHandlerRepoSpy) CreateMessage(context.Context, *events.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.createMessageCalls++
	return nil
}

func (r *messageHandlerRepoSpy) CreateReaction(context.Context, *events.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.createReactionCalls++
	return nil
}

func (r *messageHandlerRepoSpy) createMessageCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.createMessageCalls
}

func (r *messageHandlerRepoSpy) createReactionCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.createReactionCalls
}

func reactionEventForTest(eventID, targetID, emoji string) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     types.NewJID("628123456789", types.DefaultUserServer),
				Sender:   types.NewJID("628111111111", types.DefaultUserServer),
				IsFromMe: false,
			},
			ID:        eventID,
			Timestamp: time.Date(2026, time.May, 16, 8, 0, 0, 0, time.UTC),
		},
		Message: &waE2E.Message{
			ReactionMessage: &waE2E.ReactionMessage{
				Key: &waCommon.MessageKey{
					RemoteJID: protoString("628123456789@s.whatsapp.net"),
					FromMe:    protoBool(false),
					ID:        protoString(targetID),
				},
				Text: protoString(emoji),
			},
		},
	}
}

func TestShouldIgnoreImageDownload(t *testing.T) {
	tests := []struct {
		name              string
		autoDownloadMedia bool
		ignoreStatusMedia bool
		chatJID           types.JID
		expected          bool
	}{
		{
			name:              "auto download disabled",
			autoDownloadMedia: false,
			ignoreStatusMedia: false,
			chatJID:           types.StatusBroadcastJID,
			expected:          true,
		},
		{
			name:              "status media ignored",
			autoDownloadMedia: true,
			ignoreStatusMedia: true,
			chatJID:           types.StatusBroadcastJID,
			expected:          true,
		},
		{
			name:              "normal broadcast list not ignored when status ignored",
			autoDownloadMedia: true,
			ignoreStatusMedia: true,
			chatJID:           types.NewJID("123456789", types.BroadcastServer),
			expected:          false,
		},
		{
			name:              "status media downloaded when ignore disabled",
			autoDownloadMedia: true,
			ignoreStatusMedia: false,
			chatJID:           types.StatusBroadcastJID,
			expected:          false,
		},
		{
			name:              "normal user chat not ignored",
			autoDownloadMedia: true,
			ignoreStatusMedia: true,
			chatJID:           types.NewJID("123456789", types.DefaultUserServer),
			expected:          false,
		},
		{
			// whatsmeow's broadcast branch does not ToNonAD() source.Chat, so
			// the status JID can arrive device-qualified and must still match.
			name:              "device-qualified status jid ignored",
			autoDownloadMedia: true,
			ignoreStatusMedia: true,
			chatJID:           types.JID{User: "status", Server: types.BroadcastServer, Device: 1},
			expected:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldIgnoreImageDownload(tt.autoDownloadMedia, tt.ignoreStatusMedia, tt.chatJID)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
