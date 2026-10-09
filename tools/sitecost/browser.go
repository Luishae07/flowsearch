package main

import (
	"context"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// newBrowser starts one headless Chrome (the runners have it installed). Every site is then loaded in
// its own fresh tab with an empty cache, so the numbers are what a first-time visitor downloads.
func newBrowser() (context.Context, context.CancelFunc) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("no-sandbox", true), chromedp.Flag("disable-gpu", true))
	alloc, stopAlloc := chromedp.NewExecAllocator(context.Background(), opts...)
	ctx, stopCtx := chromedp.NewContext(alloc)
	if err := chromedp.Run(ctx); err != nil { // starts the browser; fails if Chrome is missing
		stopCtx()
		stopAlloc()
		return nil, nil
	}
	return ctx, func() { stopCtx(); stopAlloc() }
}

// browserMeasure loads https://host/ with JavaScript running, waits for the page to settle, and adds up
// every byte Chrome received over the network (compressed, as sent): total, JavaScript files, requests.
func browserMeasure(browser context.Context, host string) (total, js int64, requests int, err error) {
	ctx, cancel := chromedp.NewContext(browser)
	defer cancel()
	ctx, cancelT := context.WithTimeout(ctx, 40*time.Second)
	defer cancelT()

	var mu sync.Mutex
	kind := map[network.RequestID]network.ResourceType{}
	size := map[network.RequestID]float64{}
	chromedp.ListenTarget(ctx, func(ev any) {
		mu.Lock()
		defer mu.Unlock()
		switch e := ev.(type) {
		case *network.EventResponseReceived:
			kind[e.RequestID] = e.Type
		case *network.EventLoadingFinished:
			size[e.RequestID] = e.EncodedDataLength
		}
	})
	err = chromedp.Run(ctx,
		network.Enable(),
		network.SetCacheDisabled(true),
		chromedp.Navigate("https://"+host+"/"),
		chromedp.Sleep(6*time.Second), // lets scripts fetch the things they fetch after load
	)
	mu.Lock()
	defer mu.Unlock()
	for id, n := range size {
		total += int64(n)
		requests++
		if kind[id] == network.ResourceTypeScript {
			js += int64(n)
		}
	}
	return total, js, requests, err
}
