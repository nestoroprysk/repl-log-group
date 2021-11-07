package integration_test

import (
	"integration"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestReplLog(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Replication Log App Suite")
}

var _ = It("A sample message gets replicated", func() {
	m, ss := env()

	msg := makeMessage()
	Expect(m.PostMessage(msg)).To(Succeed())

	for _, c := range append(ss, m) {
		msgs, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(msgs).To(ContainElement(msg.Message))
	}
})

var _ = It("A delay is respected", func() {
	m, ss := env()

	var second uint32 = 1

	var msgs []integration.Message

	msg := makeMessage()
	msg.Secondary1.Delay = second
	msg.Secondary2.Delay = second
	msgs = append(msgs, msg)

	msg = makeMessage()
	msg.Secondary1.Delay = second
	msgs = append(msgs, msg)

	msg = makeMessage()
	msg.Secondary2.Delay = second
	msgs = append(msgs, msg)

	for _, msg := range msgs {
		takesAtLeast(func() {
			Expect(m.PostMessage(msg)).To(Succeed())
		}, time.Second)

		for _, c := range append(ss, m) {
			msgs, err := c.GetMessages()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(ContainElement(msg.Message))
		}
	}
})

var _ = It("Many messages get replicated", func() {
	m, ss := env()

	var msgs []integration.Message
	for i := 0; i < 100; i++ {
		msgs = append(msgs, makeMessage())
	}

	var wg sync.WaitGroup
	for _, _msg := range msgs {
		wg.Add(1)
		msg := _msg
		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			Expect(m.PostMessage(msg)).To(Succeed())
		}()
	}
	wg.Wait()

	expected, err := m.GetMessages()
	Expect(err).NotTo(HaveOccurred())
	for _, m := range msgs {
		Expect(expected).To(ContainElement(m.Message))
	}

	for _, c := range ss {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(ContainElements(expected))
	}
})

var _ = It("Simultaneous posts and gets don't crash the app", func() {
	m, ss := env()

	var jobs []func()
	for i := 0; i < 100; i++ {
		jobs = append(jobs, func() {
			msg := makeMessage()
			Expect(m.PostMessage(msg)).To(Succeed())
		})

		for _, c := range append(ss, m) {
			jobs = append(jobs, func() {
				_, err := c.GetMessages()
				Expect(err).NotTo(HaveOccurred())
			})
		}
	}

	var wg sync.WaitGroup
	for _, _job := range jobs {
		wg.Add(1)
		job := _job
		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			job()
		}()
	}

	wg.Wait()
})

var _ = It("Noreply is respected", func() {
	m, ss := env()

	msg := makeMessage()
	msg.Secondary1.NoReply = true
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for _, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(ContainElement(msg.Message))
	}

	msg = makeMessage()
	msg.Secondary1.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		secondary2 := i == 1
		if !secondary2 {
			Expect(result).NotTo(ContainElement(msg.Message))
		}
	}

	msg = makeMessage()
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		secondary1 := i == 0
		if !secondary1 {
			Expect(result).NotTo(ContainElement(msg.Message))
		}
	}
})

var _ = It("Even if both nodes fail and w=1, no error", func() {
	m, ss := env()

	msg := makeMessage()
	msg.WriteConcern = 1
	msg.Secondary1.NoReply = true
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).To(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())

		master := i == 2
		if master {
			Expect(result).To(ContainElement(msg.Message))
		} else {
			result, err := c.GetMessages()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(ContainElement(msg.Message))
		}
	}
})

var _ = It("The default write concern (w=3) errors if both nodes fail", func() {
	m, ss := env()

	msg := makeMessage()
	msg.Secondary1.NoReply = true
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for _, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(ContainElement(msg.Message))
	}
})

var _ = It("The explicit write concern w=3 errors if both nodes fail", func() {
	m, ss := env()

	msg := makeMessage()
	msg.WriteConcern = 3
	msg.Secondary1.NoReply = true
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for _, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(ContainElement(msg.Message))
	}
})

var _ = It("The explicit write concern w=3 errors if one node fails (secondary-1)", func() {
	m, ss := env()

	msg := makeMessage()
	msg.WriteConcern = 3
	msg.Secondary1.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		secondary2 := i == 1
		if !secondary2 {
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(ContainElement(msg.Message))
		}
	}
})

var _ = It("The explicit write concern w=3 errors if one node fails (secondary-2)", func() {
	m, ss := env()

	msg := makeMessage()
	msg.WriteConcern = 3
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		secondary1 := i == 0
		if !secondary1 {
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(ContainElement(msg.Message))
		}
	}
})

var _ = It("Write concern w=2 errors if both nodes fail", func() {
	m, ss := env()

	msg := makeMessage()
	msg.WriteConcern = 2
	msg.Secondary1.NoReply = true
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for _, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(ContainElement(msg.Message))
	}
})

var _ = It("Write concern w=2 succeeds if one of the nodes fails (secondary-1)", func() {
	m, ss := env()

	msg := makeMessage()
	msg.WriteConcern = 2
	msg.Secondary1.NoReply = true
	Expect(m.PostMessage(msg)).To(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		secondary1 := i == 0
		if secondary1 {
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(ContainElement(msg.Message))
		} else {
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(ContainElement(msg.Message))
		}
	}
})

var _ = It("Write concern w=2 succeeds if one of the nodes fails (secondary-2)", func() {
	m, ss := env()

	msg := makeMessage()
	msg.WriteConcern = 2
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).To(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		secondary2 := i == 1
		if secondary2 {
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(ContainElement(msg.Message))
		} else {
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(ContainElement(msg.Message))
		}
	}
})

var _ = It("If w=2 and secondary-2 returned the result, not waiting for secondary-1", func() {
	m, _ := env()

	msg := makeMessage()
	msg.WriteConcern = 2
	msg.Secondary1.Delay = 2 // Two seconds.

	takesAtMost(func() {
		Expect(m.PostMessage(msg)).To(Succeed())
	}, time.Second)
})

func env() (*integration.Client, []*integration.Client) {
	c, err := integration.MasterConfig()
	Expect(err).NotTo(HaveOccurred())

	m, err := integration.NewClient(c)
	Expect(err).NotTo(HaveOccurred())

	c, err = integration.Secondary1Config()
	Expect(err).NotTo(HaveOccurred())

	s1, err := integration.NewClient(c)
	Expect(err).NotTo(HaveOccurred())

	c, err = integration.Secondary2Config()
	Expect(err).NotTo(HaveOccurred())

	s2, err := integration.NewClient(c)
	Expect(err).NotTo(HaveOccurred())

	return m, []*integration.Client{s1, s2}
}

func takesAtLeast(f func(), t time.Duration) {
	begin := time.Now()
	f()
	end := time.Now()
	Expect(end).To(BeTemporally(">", begin.Add(t)), "Execution didn't take long enough")
}

func takesAtMost(f func(), t time.Duration) {
	begin := time.Now()
	f()
	end := time.Now()
	Expect(end).To(BeTemporally("<", begin.Add(t)), "Execution took too long")
}

func makeMessage() integration.Message {
	u, err := uuid.NewRandom()
	Expect(err).NotTo(HaveOccurred())
	return integration.Message{Message: u.String()}
}
