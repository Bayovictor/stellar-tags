import { useEffect, useRef, useState } from 'react';
import freighterApi from '@stellar/freighter-api';
import toast from 'react-hot-toast';
import { Client as PaymentRouterClient, networks } from '@stellar-tags/payment-router';
import { useLatencyTracker } from '../useLatencyTracker';
import { usePaymentEvents } from '../usePaymentEvents';
import LatencyGauge from '../LatencyGauge';
import NetworkBadge from '../NetworkBadge';
import { useDebounce } from '../useDebounce';
import ScrollToTop from '../ScrollToTop';
import MobileNav from './MobileNav';
import RecentAddresses from '../components/RecentAddresses';
import { useRecentAddresses } from '../useRecentAddresses';
import {
  API_BASE,
  NAV_STORAGE_KEY,
  TOKEN_ADDRESS,
  formatShortAddress,
  formatUsername,
  resolveRecipient,
  apiErrorMessage,
  useNavState,
  useWalletMenu,
} from './shared';

function Dashboard({
  userPublicKey,
  onConnectWallet,
  onDisconnectWallet,
  balance,
  isRefreshing,
  balanceError,
  onRefreshBalance,
  onRegisterClick,
  onAnalyticsClick,
  onHistoryClick,
  onHelpClick,
  onRegistrationStateChange,
  canRegister,
}) {
  const [isNavOpen, setIsNavOpen] = useNavState();
  const {
    menuRef,
    isOpen: isWalletMenuOpen,
    setIsOpen: setIsWalletMenuOpen,
  } = useWalletMenu();
  const closeNav = () => {
    sessionStorage.setItem(NAV_STORAGE_KEY, "false");
    setIsNavOpen(false);
  };
  const handleNav = (action) => {
    sessionStorage.setItem(NAV_STORAGE_KEY, 'false')
    setIsNavOpen(false)
    action()
  }
  const [nameTag, setNameTag] = useState('')
  const [showRecent, setShowRecent] = useState(false);
  const { recentAddresses, saveAddress, clearAddresses } = useRecentAddresses();
  const recipientRef = useRef(null);
  const debouncedNameTag = useDebounce(nameTag, 300)
  const [amount, setAmount] = useState('')
  const [isProcessing, setIsProcessing] = useState(false)
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [isReceiving, setIsReceiving] = useState(false)
  const [activeBalancePanel, setActiveBalancePanel] = useState('')
  const [showBalance, setShowBalance] = useState(true)
  const [receiveAddress, setReceiveAddress] = useState('')
  const [receiveTag, setReceiveTag] = useState('')
  const [receiveStatus, setReceiveStatus] = useState({
    text: '',
    color: '#1F2937',
    bgColor: '#F3F4F6',
  })
  const [status, setStatus] = useState({
    text: "",
    color: "#1F2937",
    bgColor: "#F3F4F6",
  });

  // Initialize latency tracker for real-time API monitoring
  const latencyTracker = useLatencyTracker(API_BASE);

  // #730 — Live payment status updates over SSE. The browser's EventSource
  // handles reconnection natively; we only render what arrives.
  const paymentEvents = usePaymentEvents({
    address: userPublicKey || null,
  });
  const liveStatusText =
    paymentEvents.status === 'open'
      ? paymentEvents.lastEvent
        ? `Live: ${paymentEvents.lastEvent.event === 'payment.created' ? 'payment registered' : 'payment detected'} (${paymentEvents.lastEvent.data?.amount ?? ''} ${paymentEvents.lastEvent.data?.asset === 'native' ? 'XLM' : paymentEvents.lastEvent.data?.asset ?? ''})`.replace(/\s+\)/, ')')
        : 'Live updates connected'
      : paymentEvents.status === 'connecting'
        ? 'Connecting to live updates…'
        : '';
  const liveStatusColor = paymentEvents.status === 'open' ? '#059669' : '#6B7280';
  const liveStatusBg = paymentEvents.status === 'open' ? '#D1FAE5' : '#F3F4F6';

  const walletLabel = userPublicKey
    ? `Connected: ${userPublicKey.substring(0, 5)}...${userPublicKey.substring(51)}`
    : "";

  const displayMessage = (text, color, bgColor) => {
    setStatus({ text, color, bgColor });
  };

  const displayReceiveMessage = (text, color, bgColor) => {
    setReceiveStatus({ text, color, bgColor });
  };

  useEffect(() => {
    if (!userPublicKey) {
      Promise.resolve().then(() => onRegistrationStateChange("unknown"));
      return;
    }

    const loadReceiveDetails = async () => {
      setIsReceiving(true);
      displayReceiveMessage(
        "Loading your receive details...",
        "#1F2937",
        "#F3F4F6",
      );

      try {
        const response = await fetch(
          `${API_BASE}/lookup?address=${encodeURIComponent(userPublicKey)}`,
        );
        const rawBody = await response.text();
        const data = rawBody ? JSON.parse(rawBody) : null;

        if (response.ok && data) {
          setReceiveAddress(data.address);
          setReceiveTag(data.username);
          displayReceiveMessage(
            "Share your username or wallet address.",
            "#059669",
            "#D1FAE5",
          );
          onRegistrationStateChange("existing");
          return;
        }

        if (response.status === 404) {
          setReceiveAddress(userPublicKey);
          setReceiveTag("");
          displayReceiveMessage(
            "No username found. Register to claim one.",
            "#D97706",
            "#FEF3C7",
          );
          // Change "new" to "skipped" to break the infinite redirect loop
          onRegistrationStateChange("skipped"); 
          return;
        }

        throw new Error(apiErrorMessage(data, `Backend error (${response.status}).`))
      } catch (error){
        setReceiveAddress(userPublicKey)
        setReceiveTag('')
        displayReceiveMessage(error.message || 'Unable to load receive details.', '#DC2626', '#FEE2E2')
        onRegistrationStateChange('unknown')
      } finally {
        setIsReceiving(false);
      }
    };

    loadReceiveDetails();
  }, [userPublicKey, onRegistrationStateChange]);

  useEffect(() => {
    recipientRef.current?.focus();
  }, []);

  useEffect(() => {
    if (!debouncedNameTag || !userPublicKey) {
      return;
    }

    const searchRecipient = async () => {
      try {
        const resolved = await resolveRecipient(debouncedNameTag);
        if (resolved.error) {
          return;
        }

        if (resolved.address) {
          return;
        }

        if (resolved.tag) {
          const response = await fetch(
            `${API_BASE}/federation?q=${encodeURIComponent(resolved.tag)}&type=name`,
          );
          const data = response.ok ? await response.json() : null;
          if (!data?.account_id) {
            return;
          }
        }
      } catch  {
        // Silently fail on search errors during typing
      }
    };

    searchRecipient();
  }, [debouncedNameTag, userPublicKey]);

  const handleConnect = async () => {
    const result = await onConnectWallet();
    if (!result.ok) {
      displayMessage(
        result.error || "Wallet connection failed.",
        "#DC2626",
        "#FEE2E2",
      );
      return;
    }

    displayMessage("Wallet connected.", "#059669", "#D1FAE5");
  };

  const handleLookup = async () => {
    if (isSubmitting) return;
    setIsSubmitting(true);

    const recipientInput = nameTag.trim();
    const amountValue = parseFloat(amount);

    if (!amountValue || Number.isNaN(amountValue) || amountValue <= 0) {
      toast.error("Please enter a valid amount greater than zero.");
      setIsSubmitting(false);
      return;
    }

    setIsProcessing(true);
    const toastId = toast.loading("Verifying recipient...");

    try {
      const resolved = await resolveRecipient(recipientInput);
      if (resolved.error)
        throw new Error(`Resolution error: ${resolved.error}`);

      let recipientAddress = resolved.address;
      if (!recipientAddress && resolved.tag) {
        const response = await fetch(
          `${API_BASE}/federation?q=${encodeURIComponent(resolved.tag)}&type=name`,
        );
        const data = response.ok ? await response.json() : null;
        if (!data?.account_id)
          throw new Error(
            "Recipient address could not be resolved from backend.",
          );
        recipientAddress = data.account_id;
      }

      toast.loading("Simulating smart contract execution...", { id: toastId });
      const amountStroops = BigInt(Math.floor(amountValue * 10000000));

      // Build and simulate the payment through the type-safe client generated
      // from the contract ABI (packages/types).
      const client = new PaymentRouterClient({
        ...networks.testnet,
        rpcUrl: "https://soroban-testnet.stellar.org",
      });

      let assembledTransaction;
      try {
        assembledTransaction = await client.route_payment({
          sender: userPublicKey,
          recipient: recipientAddress,
          token_address: TOKEN_ADDRESS,
          amount: amountStroops,
        });
      } catch (err) {
        throw new Error(`Simulation failed: ${err.message}`, { cause: err });
      }

      toast.loading("Approve the transaction in your wallet...", { id: toastId });
      let signedXdrResponse;
      try {
        signedXdrResponse = await freighterApi.signTransaction(
          assembledTransaction.toXDR(),
          {
            network: "TESTNET",
            networkPassphrase: "Test SDF Network ; September 2015",
          },
        );
        if (signedXdrResponse.error) throw new Error(signedXdrResponse.error);
      } catch (err) {
        throw new Error(`Wallet signature failed: ${err.message}`, {
          cause: err,
        });
      }

      toast.loading("Submitting to Stellar Testnet...", { id: toastId });
      const finalXdr =
        typeof signedXdrResponse === "string"
          ? signedXdrResponse
          : signedXdrResponse.signedTxXdr || signedXdrResponse;

      const rpcResponse = await fetch("https://soroban-testnet.stellar.org", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "sendTransaction",
          params: { transaction: finalXdr },
        }),
      });

      const rpcData = await rpcResponse.json();
      if (rpcData.error)
        throw new Error(`RPC Error: ${rpcData.error.message}`);

      const txStatus = rpcData.result?.status;
      if (txStatus === "PENDING" || txStatus === "SUCCESS") {
        const txHash = rpcData.result?.hash;
        const explorerUrl = txHash
          ? `https://stellar.expert/explorer/testnet/tx/${txHash}`
          : null;

        toast.success(
          (t) => (
            <span>
              Payment successful!
              {explorerUrl && (
                <>
                  {' '}
                  <a
                    href={explorerUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ color: '#0052FF', fontWeight: 600 }}
                    onClick={() => toast.dismiss(t.id)}
                  >
                    View on Stellar Expert
                  </a>
                </>
              )}
            </span>
          ),
          { id: toastId, duration: 8000 },
        );

        displayMessage("Payment successful!", "#059669", "#D1FAE5");
        saveAddress(recipientInput);
        setAmount("");
        onRefreshBalance();
        window.dispatchEvent(new Event("stellar:tx-update"));
      } else {
        throw new Error(
          `Blockchain rejected transaction: ${txStatus || "Unknown"}`,
        );
      }
    } catch (error) {
      toast.error(error.message || "Transaction failed.", { id: toastId });
      displayMessage(
        error.message || "A critical error occurred.",
        "#DC2626",
        "#FEE2E2",
      );
    } finally {
      setIsProcessing(false);
      setIsSubmitting(false);
    }
  };

  const handleCopy = async (value, label) => {
    if (!value) {
      return;
    }

    try {
      await navigator.clipboard.writeText(value);
      displayReceiveMessage(
        `${label} copied to clipboard.`,
        "#059669",
        "#D1FAE5",
      );
    } catch {
      displayReceiveMessage(
        "Copy failed. Please copy manually.",
        "#DC2626",
        "#FEE2E2",
      );
    }
  };

  const handleDisconnect = () => {
    setIsWalletMenuOpen(false);
    onDisconnectWallet();
    displayMessage("Wallet disconnected.", "#1F2937", "#F3F4F6");
  };

  return (
    <div className={`dashboard ${isNavOpen ? "nav-open" : ""}`}>
      <button
        type="button"
        className={`sidebar-scrim ${isNavOpen ? "is-open" : ""}`}
        onClick={() => setIsNavOpen(false)}
        aria-label="Close navigation"
      />
      <aside className={`sidebar ${isNavOpen ? "is-open" : ""}`}>
        <div className="brand">
          <div className="brand-mark">S</div>
          <h1>Stellar Pay</h1>
        </div>
        <div className="nav">
          <button type="button" aria-current="page" onClick={closeNav}>
            Dashboard
          </button>
          <button type="button" onClick={() => handleNav(onHistoryClick)}>
            History
          </button>
          <button type="button" onClick={() => handleNav(onAnalyticsClick)}>
            Analytics
          </button>
          <button type="button" onClick={() => handleNav(onHelpClick)}>
            Help
          </button>
          {canRegister && (
            <button type="button" onClick={() => handleNav(onRegisterClick)}>
              Registration
            </button>
          )}
        </div>
        <div className="sidebar-card">
          <h3>Network pulse</h3>
          <p>Testnet status is healthy. Avg confirmation 3.9s.</p>
        </div>
        <div className="sidebar-card">
          <h3>Support</h3>
          <p>Need a hand? Open the help panel for quick answers.</p>
        </div>
        {userPublicKey && (
          <button
            type="button"
            className="disconnect-button"
            onClick={handleDisconnect}
          >
            Disconnect wallet
          </button>
        )}
      </aside>

      <main className="main">
        <section className="topbar reveal">
          <button
            type="button"
            className="hamburger"
            onClick={() => setIsNavOpen((prev) => !prev)}
            aria-label="Toggle navigation"
            aria-expanded={isNavOpen}
          >
            <span />
            <span />
            <span />
          </button>
          <div>
            <h2 className="headline">Frictionless Stellar Finance.</h2>
            <p className="subtle">
              Experience frictionless finance with verified identities and
              real-time smart routing.
            </p>
          </div>
          <div className="topbar-actions">
            <NetworkBadge />
            <LatencyGauge 
              latency={latencyTracker.latency}
              status={latencyTracker.status}
            />
            <div className="wallet-menu" ref={menuRef}>
              <button
                type="button"
                className="connect-pill"
                onClick={() => {
                  if (userPublicKey) {
                    setIsWalletMenuOpen((prev) => !prev);
                  } else {
                    handleConnect();
                  }
                }}
                aria-expanded={isWalletMenuOpen}
              >
                {userPublicKey
                  ? `Connected: ${formatShortAddress(userPublicKey)}`
                  : "Connect wallet"}
              </button>
              {userPublicKey && isWalletMenuOpen && (
                <div className="wallet-dropdown">
                  <button type="button" onClick={handleDisconnect}>
                    Disconnect wallet
                  </button>
                </div>
              )}
            </div>
          </div>
        </section>

        <section className="grid columns-1 balance-grid">
          <div className="card reveal balance-card">
            <div className="card-header">
              <h2>Current balance</h2>
              <button
                type="button"
                className={`refresh-button ${isRefreshing ? "is-loading" : ""}`}
                onClick={onRefreshBalance}
                disabled={!userPublicKey || isRefreshing}
                aria-label="Refresh balance"
              >
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path d="M20 12a8 8 0 1 1-2.34-5.66" />
                  <path d="M20 4v6h-6" />
                </svg>
              </button>
            </div>
            {balanceError && (
              <div className="balance-error">{balanceError}</div>
            )}
            <div className="metric">
              {userPublicKey && balance !== null
                ? showBalance
                  ? balance.toLocaleString(undefined, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })
                  : "***"
                : "--"}{" "}
              <span>XLM</span>
              <button
                type="button"
                className="privacy-toggle"
                onClick={() => setShowBalance(!showBalance)}
                aria-label={showBalance ? "Hide balance" : "Show balance"}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  {showBalance ? (
                    <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-7-11-9 0-2.3 1.52-4.58 3.88-6.35M21 9c0 2 0 7-9 7-.71 0-1.4-.07-2.07-.2M21 3l-6 6m6 6l6 6" />
                  ) : (
                    <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8zM12 15a3 3 0 0 0 3-3 3 3 0 0 0-3-3 3 3 0 0 0-3 3 3 3 0 0 0 3 3z" />
                  )}
                </svg>
              </button>
            </div>
            <div className="balance-tabs">
              <button
                type="button"
                className={activeBalancePanel === "transfer" ? "is-active" : ""}
                onClick={() => setActiveBalancePanel("transfer")}
              >
                Transfer
              </button>
              <button
                type="button"
                className={activeBalancePanel === "receive" ? "is-active" : ""}
                onClick={() => setActiveBalancePanel("receive")}
              >
                Receive
              </button>
            </div>
            {activeBalancePanel === "transfer" && (
              <div className="balance-panel">
                {!userPublicKey && (
                  <div className="wallet-status">
                    Connect your wallet to make a transfer.
                  </div>
                )}
                {walletLabel && (
                  <div className="wallet-status">{walletLabel}</div>
                )}
                <label>Recipient username or address</label>
                <div style={{ position: 'relative' }}>
                  <input
                    ref={recipientRef}
                    type="text"
                    value={nameTag}
                    onChange={(event) => {
                      setNameTag(event.target.value);
                      setShowRecent(true);
                    }}
                    onFocus={() => setShowRecent(true)}
                    placeholder="e.g., walzeem or G..."
                    autoComplete="off"
                    disabled={!userPublicKey || isProcessing}
                  />
                  <RecentAddresses
                    addresses={recentAddresses}
                    isVisible={showRecent && recentAddresses.length > 0}
                    onSelect={(address) => {
                      setNameTag(address);
                      setShowRecent(false);
                      recipientRef.current?.focus();
                    }}
                    onClear={clearAddresses}
                    onClose={() => setShowRecent(false)}
                  />
                </div>
                {nameTag === userPublicKey && (
                  <span className="field-error">Warning: You are sending funds to your own address.</span>
                )}

                <label>Amount (XLM)</label>
                <input
                  type="text"
                  inputMode="decimal"
                  value={amount}
                  onChange={(event) => {
                    const raw = event.target.value.replace(/[^0-9.]/g, "");
                    const parts = raw.split(".");
                    const cleaned =
                      parts.length > 2
                        ? parts[0] + "." + parts.slice(1).join("")
                        : raw;
                    setAmount(cleaned);
                  }}
                  placeholder="0.00"
                  disabled={!userPublicKey || isProcessing}
                />

                <div className="form-actions">
                <button
                  type="button"
                  className="accent-btn disabled:opacity-50 disabled:cursor-not-allowed"
                  onClick={handleLookup}
                  disabled={
                    !userPublicKey ||
                    isSubmitting ||
                    !amount ||
                    Number(amount) <= 0
                  }
                >
                  {isSubmitting ? "Processing..." : "Transfer"}
                </button>
                  <button
                    type="button"
                    className="ghost-button"
                    onClick={() => {
                      setNameTag("");
                      setAmount("");
                    }}
                    disabled={isProcessing}
                  >
                    Clear
                  </button>
                </div>
              </div>
            )}
            {activeBalancePanel === "receive" && (
              <div className="balance-panel">
                {!userPublicKey && (
                  <div className="wallet-status">
                    Connect your wallet to view receive details.
                  </div>
                )}
                {userPublicKey && receiveAddress && (
                  <div className="receive-panel">
                    {receiveTag && (
                      <div className="wallet-status">
                        Username: {formatUsername(receiveTag)}
                      </div>
                    )}
                    <div className="wallet-status">
                      Address: {receiveAddress}
                    </div>
                    <div className="inline-actions">
                      {receiveTag && (
                        <button
                          type="button"
                          className="ghost-button"
                          onClick={() =>
                            handleCopy(formatUsername(receiveTag), "Username")
                          }
                          disabled={isReceiving}
                        >
                          Copy username
                        </button>
                      )}
                      <button
                        type="button"
                        className="ghost-button"
                        onClick={() => handleCopy(receiveAddress, "Address")}
                        disabled={isReceiving}
                      >
                        Copy address
                      </button>
                    </div>
                    <div className="qr-card">
                      <img
                        src={`https://api.qrserver.com/v1/create-qr-code/?size=160x160&data=${encodeURIComponent(receiveAddress)}`}
                        alt="Wallet address QR code"
                      />
                    </div>
                  </div>
                )}
                {receiveStatus.text && (
                  <div
                    id="receive-status-box"
                    style={{
                      color: receiveStatus.color,
                      backgroundColor: receiveStatus.bgColor,
                      marginTop: "16px", // Adds a little breathing room below the QR code!
                    }}
                  >
                    {receiveStatus.text}
                  </div>
                )}
              </div>
            )}
            {liveStatusText && (
              <div
                id="live-status-box"
                style={{
                  color: liveStatusColor,
                  backgroundColor: liveStatusBg,
                  marginTop: "8px",
                  fontSize: "0.85em",
                }}
                title="Streamed live from the backend via Server-Sent Events"
              >
                {liveStatusText}
              </div>
            )}
            {status.text && (
              <div
                id="status-box"
                style={{ color: status.color, backgroundColor: status.bgColor }}
              >
                {status.text}
              </div>
            )}
          </div>
        </section>
      </main>
      <MobileNav
        active="dashboard"
        onDashboardClick={closeNav}
        onHistoryClick={() => handleNav(onHistoryClick)}
        onAnalyticsClick={() => handleNav(onAnalyticsClick)}
        onHelpClick={() => handleNav(onHelpClick)}
        onRegisterClick={() => handleNav(onRegisterClick)}
        canRegister={canRegister}
      />
      <ScrollToTop />
    </div>
  );
}

export default Dashboard;
