/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/
package im.tym.camel.j8.async;

import org.apache.camel.*;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.util.ServiceHelper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * @author vit@tym.im
 */
public class AsyncExchangeSender {
    private CamelContext camelContext;
    private Endpoint endpoint;
    private SendProcessor sendProcessor;
    private boolean checkForException = true;

    CompletableFuture<Exchange> send(final Exchange exchange) {
        ensureEnpoint();
        final CompletableFuture<Exchange> future = new CompletableFuture<Exchange>();
        sendProcessor.process(exchange, new AsyncCallback() {
            public void done(boolean doneSync) {
                if (checkForException) {
                    Exception exception = exchange.getException();
                    if (exception != null) {
                        future.completeExceptionally(exception);
                        return;
                    }
                }
                future.complete(exchange);
            }
        });
        return future;
    }

    CompletableFuture<Exchange> sendBody(Object body) {
        ensureEnpoint();
        Exchange exchange = endpoint.createExchange();
        exchange.getIn().setBody(body);
        return send(exchange);
    }

    CompletableFuture<Exchange> sendBody(Object body, ExchangePattern pattern) {
        ensureEnpoint();
        Exchange exchange = endpoint.createExchange(pattern);
        exchange.getIn().setBody(body);
        return send(exchange);
    }

    private void ensureEnpoint() {
        if (endpoint == null) {
            throw new NullPointerException("Endpoint must be set before calling send or sendBody");
        }
    }
    public CamelContext getCamelContext() {
        return camelContext;
    }

    public void setCamelContext(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(Endpoint endpoint) throws CamelException {
        this.endpoint = endpoint;
        if (endpoint != null) {
            try {
                if (this.sendProcessor != null) {
                    ServiceHelper.stopService(this.sendProcessor);
                }
                this.camelContext = endpoint.getCamelContext();
                this.sendProcessor = new SendProcessor(endpoint);
                ServiceHelper.startService(sendProcessor);
            } catch (Exception e) {
                throw new CamelException(e);
            }
        } else {
            this.camelContext = null;
            this.sendProcessor = null;
        }
    }

    public boolean isCheckForException() {
        return checkForException;
    }

    public void setCheckForException(boolean checkForException) {
        this.checkForException = checkForException;
    }

    public AsyncExchangeSender withCamelContext(CamelContext camelContext) {
        setCamelContext(camelContext);
        return this;
    }

    public AsyncExchangeSender withEndpoint(Endpoint endpoint) throws CamelException {
        setEndpoint(endpoint);
        return this;
    }

    public AsyncExchangeSender withEnpointURI(String uri) throws CamelException {
        if (camelContext == null) {
            throw new IllegalStateException("Can not retrieve enpoint by URI without camel context. Use withCamelContext first");
        }
        setEndpoint(camelContext.getEndpoint(uri));
        return this;
    }

    public AsyncExchangeSender withCheckForException(boolean checkForException) {
        setCheckForException(checkForException);
        return this;
    }

}
