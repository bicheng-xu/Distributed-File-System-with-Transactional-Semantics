import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;


public class RequestHandler extends Thread {
	
	public static final int kTimeOutSecs = 20;
	
	public static class RequestMessage{
		String method;
		String txn_id;
		String seq_num;
		String length;
		String data;
	}
	
	public class ResponseMessage{
		String method;
		String txn_id;
		String seq_num;
		String error_code;
		String length;
		String reason;
		
		public ResponseMessage(String method, String txnId)
		{
			this.method = method;
			this.txn_id = txnId;
		}
		
		public ResponseMessage(String method, String txnId, String seqNum)
		{
			this.method = method;
			this.txn_id = txnId;
			this.seq_num = seqNum;
		}
		
		public ResponseMessage(String method, String txnId, String seqNum, String errorCode)
		{
			this.method = method;
			this.txn_id = txnId;
			this.seq_num = seqNum;
			this.error_code = errorCode;
		}
		
		public ResponseMessage(String method, String txnId, String seqNum, String errorCode, String rson)
		{
			this.method = method;
			this.txn_id = txnId;
			this.seq_num = seqNum;
			this.error_code = errorCode;
			this.length = String.valueOf(rson.length());
			this.reason = rson;
		}
		
		public String toString()
		{
			StringBuilder sb = new StringBuilder();
			sb.append(method);
			sb.append(" ");
			sb.append(txn_id);
			sb.append(" ");
			sb.append((seq_num == null)? "0" : seq_num);
			sb.append(" ");
			sb.append((error_code == null)? "0" : error_code);
			sb.append(" ");
			sb.append((length == null)? "0" : length);
			sb.append("\r\n\r\n");
			sb.append((reason != null)? reason : "\r\n");
			
			return sb.toString();
		}
	}
	
	final static int BUF_LEN = 1024; 
	//final static String delim1 = "\t\t";
	//final static String delim2 = "\t\t\t\t";
	//final static String[] state = {"NEWTXN", "WRITING", "RESENDING", "COMMITTED", "ABORTED"};
	static File txnLog;
	static FileOutputStream fosLog;	
	//static HashMap<String, String> recTxnTable;
	
	DataInputStream in_;
	DataOutputStream out_;
	File logFile_;
	FileOutputStream fos_;
	Socket socket_;
	String dir_;
	
	public RequestHandler(Socket s, String path){
		this.socket_ = s;
		//recTxnTable = recTable;
		try{
			in_ = new DataInputStream(socket_.getInputStream());
			out_ = new DataOutputStream(socket_.getOutputStream());
			dir_ = path;
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public void run()
	{				
		
		RequestMessage req = null;
		
		while (req == null)
		{
			req = readMsg();
		}
		
		String method = req.method;
		if (method.equals("READ"))
		{				
			doRead(req.data);				
		}
		else if (method.equals("NEW_TXN"))
		{
			doNewTxn(req);
		}
		else if (method.equals("WRITE"))
		{
			doWrite(req);
		}
		else if (method.equals("COMMIT"))
		{
			doCommit(req);
		}
		else if (method.equals("ABORT"))
		{
			doAbort(req.txn_id);
		}
		else
		{
			handleInvalidOperation(req.txn_id, "Wrong Method!");
		}
		
	}	

	private void doRead(String filePath)
	{
		if (filePath.isEmpty())
		{
			handleFileNameMissing();
			return;
		}
		
		String readFilePath = TxnTable.combinePath(dir_, filePath);
		if (!Files.exists(Paths.get(readFilePath)))
		{
			handleFileNotFound();
			return;
		}
		
		try
		{
			byte content[] = Files.readAllBytes(Paths.get(readFilePath));
			out_.write(content);
		}
		catch(FileNotFoundException e)
		{
			handleFileNotFound();
		}
		catch(IOException e)
		{
			handleFileIOError("-1");
		}
	}
	
	private void doNewTxn(RequestMessage req)
	{
		if (req.data.isEmpty())
		{
			handleFileNameMissing();
			return;
		}
		
		String txnId = TxnTable.getInstance().addNewTxn(req);
		sendACK(txnId);
		TxnTable.getInstance().startTimer(txnId, kTimeOutSecs);
	}
	
	private void doWrite(RequestMessage req)
	{		
		TxnTable txnTable = TxnTable.getInstance();
		if (!txnTable.isValidTxn(req.txn_id))
		{
			handleInvalidTransactionID(req.txn_id);
			return;
		}
		
		txnTable.cancelTimer(req.txn_id);
		
		TxnTable.State state = txnTable.table_.get(req.txn_id).state_;
		if (state == TxnTable.State.COMMITTED || state == TxnTable.State.ABORTTED)
		{
			handleInvalidOperation(req.txn_id, "The transaction has been committed or abortted!");
		}
		else if (state == TxnTable.State.RESENDING)
		{
			txnTable.addNewRequest(req);
			if (txnTable.getNumMissingPackets(req.txn_id) == 0)
			{
				commitTxn(this, req.txn_id);
				afterCommit(req.txn_id);
			}
			txnTable.startTimer(req.txn_id,  kTimeOutSecs);
		}
		else
		{
			txnTable.addNewRequest(req);			
			txnTable.startTimer(req.txn_id,  kTimeOutSecs);
		}
	}	
	
	private void doCommit(RequestMessage req)
	{
		TxnTable txnTable = TxnTable.getInstance();
		String txnId = req.txn_id;
		if (!txnTable.isValidTxn(txnId))
		{
			handleInvalidTransactionID(txnId);
			return;
		}
		
		txnTable.cancelTimer(req.txn_id);
		
		if (txnTable.getState(txnId) == TxnTable.State.COMMITTED)
		{
			sendACK(txnId);
		}
		else if (txnTable.getState(txnId) == TxnTable.State.ABORTTED)
		{
			handleInvalidOperation(txnId, "The transaction has been abortted");
		}
		else if (txnTable.getState(txnId) == TxnTable.State.UNCOMMITTED)
		{
			int totalWrites = Integer.parseInt(req.seq_num);
			txnTable.updateTxnLog(txnId, req);
			if (txnTable.isReadyCommit(txnId, totalWrites))
			{
				commitTxn(this, txnId);
				afterCommit(txnId);
			}
			else
			{
				doResend(txnId, totalWrites);
			}
		}
		else if (txnTable.getState(txnId) == TxnTable.State.RESENDING)
		{
			HashSet<String> missSet = new HashSet<String>();
			txnTable.getMissSeq(txnId, missSet);
			for (String s : missSet)
			{
				sendResendMsg(txnId, s);
			}
			txnTable.startTimer(txnId,  kTimeOutSecs);
		}
	}
	
	private void doResend(String txnId, int totalWrites){
		
		TxnTable txnTable = TxnTable.getInstance();
		List<RequestMessage> reqQueue = new ArrayList<RequestMessage>();
		txnTable.getAllRequests(txnId, reqQueue);
		
		Collections.sort(reqQueue, sortBySeqNum());
		int offset = 1;
		int i = 0;
		for (; i < reqQueue.size(); i++)
		{
			int seq = Integer.parseInt(reqQueue.get(i).seq_num);
			int dif = seq - offset - i;
			if (dif != 0){
				for (int j = seq - dif; j < seq; j++){
					String s = Integer.toString(j);
					txnTable.addMissingPacket(txnId, s);
					sendResendMsg(txnId,s);
				}
			}
			offset += dif;
		} 
		
		i = i + offset;
		
		while (i <= totalWrites)
		{
			String s = Integer.toString(i);
			txnTable.addMissingPacket(txnId, s);
			sendResendMsg(txnId,s);
			i++;
		}
		txnTable.updateServerLog(txnId, TxnTable.State.RESENDING);
		txnTable.updateState(txnId, TxnTable.State.RESENDING);
		txnTable.startTimer(txnId,  kTimeOutSecs);
	}
	
	private static synchronized void commitTxn(RequestHandler rh, String txnId){		
		try 
		{
			TxnTable txnTable = TxnTable.getInstance();
			String filePath = txnTable.getFilePath(txnId);
			File file = new File(filePath);
			FileOutputStream fos = new FileOutputStream(file, true);
			BufferedOutputStream bos = new BufferedOutputStream(fos);
			
			List<RequestMessage> reqQueue = new ArrayList<RequestMessage>();
			txnTable.getAllRequests(txnId, reqQueue);
			Collections.sort(reqQueue, sortBySeqNum());
			for(int i = 0; i < reqQueue.size(); i++){
				String data = reqQueue.get(i).data;
				bos.write(data.getBytes());
			}
			bos.flush();
			bos.close();
			fos.close();
			txnTable.updateState(txnId, TxnTable.State.COMMITTED);
			txnTable.updateServerLog(txnId, TxnTable.State.COMMITTED);
			txnTable.deleteTxnLog(txnId);
		} catch (FileNotFoundException e) {
			rh.handleFileNotFound();
		} catch (IOException e) {
			rh.handleFileIOError(txnId);
		}	
	}
	
	private void afterCommit(String txnId){	
		sendACK(txnId);
	}
	
	private void doAbort(String txnId)
	{	
		TxnTable txnTable = TxnTable.getInstance();

		if (!txnTable.isValidTxn(txnId))
		{
			handleInvalidTransactionID(txnId);
			return;
		}	
		
		txnTable.cancelTimer(txnId);
		
		if (txnTable.getState(txnId) == TxnTable.State.COMMITTED)
		{
			handleInvalidOperation(txnId, "The transaction has been committed");
		}
		else if (txnTable.getState(txnId) == TxnTable.State.ABORTTED)
		{
			sendACK(txnId);
		}
		else
		{
			sendACK(txnId);
			TxnTable.getInstance().updateState(txnId, TxnTable.State.ABORTTED);
			TxnTable.getInstance().updateServerLog(txnId, TxnTable.State.ABORTTED);
			TxnTable.getInstance().deleteTxnLog(txnId);
		}
	}
	
	/*==========================================
	 * Support Methods
	 * =========================================
	 */
	//Parse Request Message from Clients
	private RequestMessage readMsg()
	{
		RequestMessage rm = new RequestMessage();
		byte byteArray[] = new byte[BUF_LEN];
		int bytesRead = 0;
		
		try {
			StringBuilder sb = new StringBuilder();
			socket_.setSoTimeout(100);
			try
			{
				while ( (bytesRead = in_.read(byteArray, 0, BUF_LEN)) > 0)
				{
					sb.append(new String(byteArray, 0, bytesRead, "UTF-8"));
				}
			}
			catch(SocketTimeoutException ste)
			{
				
			}
			socket_.setSoTimeout(0);
			
			final String delim = "\r\n\r\n";
			int index = sb.toString().indexOf(delim);
			if (index < 0)
			{
				return null;
			}
			String header = sb.toString().substring(0, index);
			String[] headerParts = header.split(" ");
			if(headerParts.length < 1){
				handleInvalidMessageFormat();
				return null;
			}
			
			rm.method = headerParts[0].toUpperCase();
			
			if (headerParts.length > 1)
			{
				rm.txn_id = headerParts[1];
			}
			if (headerParts.length > 2)
			{
				rm.seq_num = headerParts[2];
			}
			if (headerParts.length > 3)	
			{
				rm.length = headerParts[3];
			}
			
			if (rm.length != null)
			{
				int len = Integer.parseInt(rm.length);
				int pos = index + delim.length();
				rm.data = sb.toString().substring(pos, pos + len);
			}
		}
		catch (IOException e) {
			// e.printStackTrace();
			rm = null;
		}

		return rm;
	}
	
	
	private void sendACK(String txnId){
		ResponseMessage resp = new ResponseMessage("ACK", txnId);
		try 
		{
			out_.writeBytes(resp.toString());
		} catch (IOException e) 
		{
			// e.printStackTrace();
		}		
	}
	
	private void sendResendMsg(String txnId, String seqNum){
		ResponseMessage resp = new ResponseMessage("ASK_RESEND", txnId, seqNum);
		try 
		{
			out_.writeBytes(resp.toString());
		} catch (IOException e) 
		{
			e.printStackTrace();
		}		
	}
	
	private static Comparator<RequestMessage> sortBySeqNum()
	{
		Comparator<RequestMessage> comp = new Comparator<RequestMessage>(){
			@Override
			public int compare(RequestMessage rm1, RequestMessage rm2)
			{
				int seq1 = Integer.parseInt(rm1.seq_num);
				int seq2 = Integer.parseInt(rm2.seq_num);
				return (seq1 > seq2) ? 1 : -1;
			}
		};
		return comp;
	}
	
	//===========================================
	//Error Handlers
	//===========================================
		
	//201: Invalid Transaction ID
	private void handleInvalidTransactionID(String txnId)
	{
		ResponseMessage resp = new ResponseMessage("ERROR", txnId, "0", "201", "Invalid Transaction ID");
		try {
			out_.writeBytes(resp.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//202: Invalid operation
	private void handleInvalidOperation(String txnId, String msg)
	{
		ResponseMessage resp = new ResponseMessage("ERROR", txnId, "0", "202", msg);
		try {
			out_.writeBytes(resp.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//205: FileIO Error
	private void handleFileIOError(String txnId)
	{
		ResponseMessage resp = new ResponseMessage("ERROR", txnId, "0", "205", "File I/O Error");
		try {
			out_.writeBytes(resp.toString());			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//206: FileNotFound Error
	private void handleFileNotFound()
	{
		ResponseMessage resp = new ResponseMessage("ERROR", "0", "0", "206", "File Not Found");
		try {
			out_.writeBytes(resp.toString());			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//207: Invalid Message Format Error
	private void handleInvalidMessageFormat()
	{
		ResponseMessage resp = new ResponseMessage("ERROR", "0", "0", "207", "Invalid Message Format");
		try {
			out_.writeBytes(resp.toString());			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	//208: FileNameMissing Error
	private void handleFileNameMissing()
	{
		ResponseMessage resp = new ResponseMessage("ERROR", "0", "0", "208", "File Name Missing");
		try {
			out_.writeBytes(resp.toString());			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
}
