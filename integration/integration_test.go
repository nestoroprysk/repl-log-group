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
	msg := makeMessage()
	msg.Delay = second
	takesAtLeast(func() {
		Expect(m.PostMessage(msg)).To(Succeed())
	}, time.Second)

	for _, c := range append(ss, m) {
		msgs, err := c.GetMessages()
		Expect(err).NotTo(HaveOccurred())
		Expect(msgs).To(ContainElement(msg.Message))
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

func makeMessage() integration.Message {
	u, err := uuid.NewRandom()
	Expect(err).NotTo(HaveOccurred())
	return integration.Message{Message: u.String()}
}
