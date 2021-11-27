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
	msg.Secondary1.Delay = 2 * second
	msg.Secondary2.Delay = 2 * second
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

		takesAtMost(func() {
			Expect(m.PostMessage(msg)).To(Succeed())
		}, 3*time.Second) // Master works in parallel (so two delays of 2 seconds don't count to 4).

		for _, c := range append(ss, m) {
			msgs, err := c.GetMessages()
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(ContainElement(msg.Message))
		}
	}
})

var _ = It("Total order is respected", func() {
	m, ss := env()

	var msgs []integration.Message
	var expected []string
	for i := 0; i < 100; i++ {
		m := makeMessage()
		msgs = append(msgs, m)
		expected = append(expected, m.Message)
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

	// Master generally doesn't have the same order as `msgs` because there are no guarantees when an exact message will get to master.
	// So we're asserting here that all the messages eventually get to master (even if in a scrumbled order).
	masterMessages, err := m.GetMessages()
	Expect(err).NotTo(HaveOccurred())
	Expect(masterMessages).To(ContainElements(expected))

	for _, c := range ss {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(masterMessages), "All the replicas should have messages in the order that's defined by master")
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

	msg = makeMessage()
	msg.Secondary1.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())

		master := i == 2
		secondary1 := i == 0
		if master {
			Expect(result).To(ContainElement(msg.Message))
		} else if secondary1 {
			Expect(result).NotTo(ContainElement(msg.Message))
		}
	}

	msg = makeMessage()
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())

		master := i == 2
		secondary2 := i == 1
		if master {
			Expect(result).To(ContainElement(msg.Message))
		} else if secondary2 {
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

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())

		master := i == 2
		if master {
			Expect(result).To(ContainElement(msg.Message))
		} else {
			Expect(result).NotTo(ContainElement(msg.Message))
		}
	}
})

var _ = It("The explicit write concern w=3 errors if both nodes fail", func() {
	m, ss := env()

	msg := makeMessage()
	msg.WriteConcern = 3
	msg.Secondary1.NoReply = true
	msg.Secondary2.NoReply = true
	Expect(m.PostMessage(msg)).NotTo(Succeed())

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())

		master := i == 2
		if master {
			Expect(result).To(ContainElement(msg.Message))
		} else {
			Expect(result).NotTo(ContainElement(msg.Message))
		}
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
		Expect(err).NotTo(HaveOccurred())

		master := i == 2
		secondary1 := i == 0
		if master {
			Expect(result).To(ContainElement(msg.Message))
		} else if secondary1 {
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
		Expect(err).NotTo(HaveOccurred())

		master := i == 2
		secondary2 := i == 1
		if master {
			Expect(result).To(ContainElement(msg.Message))
		} else if secondary2 {
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

	for i, c := range append(ss, m) {
		result, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())

		master := i == 2
		if master {
			Expect(result).To(ContainElement(msg.Message))
		} else {
			Expect(result).NotTo(ContainElement(msg.Message))
		}
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
	msg.Secondary1.Delay = 3 // In seconds.

	takesAtMost(func() {
		Expect(m.PostMessage(msg)).To(Succeed())
	}, 2*time.Second)

	time.Sleep(4 * time.Second) // Sleeping not to corrupt the other tests.
})

var _ = It("If w=2 and secondary-1 returned the result, not waiting for secondary-2", func() {
	m, _ := env()

	msg := makeMessage()
	msg.WriteConcern = 2
	msg.Secondary2.Delay = 3 // In seconds.

	takesAtMost(func() {
		Expect(m.PostMessage(msg)).To(Succeed())
	}, 2*time.Second)

	time.Sleep(4 * time.Second) // Sleeping not to corrupt the other tests.
})

var _ = It("If w=1, not waiting at all", func() {
	m, _ := env()

	msg := makeMessage()
	msg.WriteConcern = 1
	msg.Secondary1.Delay = 3 // In seconds.
	msg.Secondary2.Delay = 3 // In seconds.

	takesAtMost(func() {
		Expect(m.PostMessage(msg)).To(Succeed())
	}, 1*time.Second)

	time.Sleep(4 * time.Second) // Sleeping not to corrupt the other tests.
})

var _ = It("Messages get delivered only after previous messages are delivered", func() {
	m, ss := env()

	// Posting the first message.
	a := makeMessage()
	Expect(m.PostMessage(a)).To(Succeed())

	// Posting the second message with a huge delay for secondaries.
	b := makeMessage()
	b.WriteConcern = 1     // Master doesn't wait for secondaries.
	b.Secondary1.Delay = 3 // In seconds.
	b.Secondary2.Delay = 3 // In seconds.
	takesAtMost(func() {
		Expect(m.PostMessage(b)).To(Succeed())
	}, 1*time.Second)

	msgs, err := m.GetMessages()
	Expect(err).NotTo(HaveOccurred())
	Expect(msgs).To(Equal([]string{a.Message, b.Message}), "Master should show all the messages, even those that are not yet delivered to secondaries")

	for _, s := range ss {
		msgs, err := s.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(msgs).To(Equal([]string{a.Message}), "Secondaries shouldn't show the second message yet because of delays")
	}

	// Posting the third message without delays that should get to secondaries before the second message.
	c := makeMessage()
	Expect(m.PostMessage(c)).To(Succeed())

	msgs, err = m.GetMessages()
	Expect(err).NotTo(HaveOccurred())
	Expect(msgs).To(Equal([]string{a.Message, b.Message, c.Message}), "Master should show all the messages once again")

	for _, s := range ss {
		msgs, err := s.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(msgs).To(Equal([]string{a.Message}), "Secondaries shouldn't show the third message before the second one gets delivered")
	}

	Eventually(func(g Gomega) {
		for _, s := range ss {
			msgs, err := s.GetMessages()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(msgs).To(Equal([]string{a.Message, b.Message, c.Message}), "Secondaries should eventually deliver all the messages in the right order")
		}
	}, 4*time.Second /* timeout */, 500*time.Millisecond /* polling interval */).Should(Succeed())
})

var _ = It("Secondaries deduplicate by ID (this test involves internals)", func() {
	_, ss := env()

	msg := makeMessage()
	msg.ID = 0

	for i := 0; i < 10; i++ {
		for _, s := range ss {
			Expect(s.PostMessage(msg)).To(Succeed())
		}
	}

	for _, s := range ss {
		msgs, err := s.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(msgs).To(Equal([]string{msg.Message}))
	}
})

var _ = It("Secondaries deliver in the ID order starting from zero (this test involves internals)", func() {
	_, ss := env()
	s := ss[0]

	a := makeMessage()
	a.ID = 1

	Expect(s.PostMessage(a)).To(Succeed())

	msgs, err := s.GetMessages()
	Expect(err).NotTo(HaveOccurred())
	Expect(msgs).To(BeEmpty(), "Message with ID 1 shouldn't be delivered before ID zero is")

	b := makeMessage()
	b.ID = 0

	Expect(s.PostMessage(b)).To(Succeed())

	msgs, err = s.GetMessages()
	Expect(err).NotTo(HaveOccurred())
	Expect(msgs).To(Equal([]string{b.Message, a.Message}), "Secondaries should eventually deliver both messages in the right order")
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

	ss := []*integration.Client{s1, s2}
	for _, c := range append(ss, m) {
		c.Flush()
	}

	return m, ss
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
