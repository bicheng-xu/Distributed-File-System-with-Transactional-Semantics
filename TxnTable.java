import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Timer;
import java.util.TimerTask;

public class TxnTable 
{
	public enum State
	{
		UNCOMMITTED, 
		RESENDING,
		COMMITTED, 
		ABORTTED
	}
	
	public class TxnData
	{		
		String txnId_;
		String filePath_ = "";	
		List<RequestHandler.RequestMessage> reqQueue_;
		HashSet<String> seqSet_;
		HashSet<String> missSeqSet_;
		State state_;
		File txnLog_;
		FileOutputStream txnOut_;
		String recoverTotal_;
		Timer timer_;

		public TxnData(String txnId, String path)
		{
			txnId_ = txnId;
			state_ = State.UNCOMMITTED;
			filePath_ = path;
			reqQueue_ = new ArrayList<RequestHandler.RequestMessage>();
			seqSet_ = new HashSet<String>();
			missSeqSet_ = new HashSet<String>();
		}
		
		public TxnData(String txnId, State state)
		{
			txnId_ = txnId;
			state_ = state;
		}
		
		public void startTimer(int timeInSecs)
		{
			timer_ = new Timer();
			
			timer_.schedule(new TimerTask() 
			{
				@Override
				public void run()
				{
					// Abort
					System.out.println("Timed out for transaction " + txnId_);
					state_ = TxnTable.State.ABORTTED;
					if (reqQueue_ != null)
					{
						reqQueue_.clear();
					}
					if (seqSet_ != null)
					{
						seqSet_.clear();
						
					}
					if (missSeqSet_ != null)
					{
						missSeqSet_.clear();
					}
					
					TxnTable.getInstance().updateServerLog(txnId_, State.ABORTTED);
					if (txnLog_ != null)
					{
						txnLog_.delete();
						txnOut_ = null;
					}
					
				}
			}, timeInSecs * 1000); 
		}
		
		public void cancelTimer()
		{
			if (timer_ != null)
			{
				timer_.cancel();
			}
		}
	}
	
	static TxnTable instance_ = null;
	
	final static String delim1 = "\t\t";
	final static String delim2 = "\t\t\t\t";
	HashMap<String, TxnData> table_;
	static int s_txnId = 1000;	
	File serverLog_;
	FileOutputStream serverOut_;
	String dir_;
	
	private TxnTable()
	{
		table_ = new HashMap<String, TxnData>();
	}
	
	
	public static TxnTable getInstance()
	{
		if (instance_ == null)
		{
			instance_ = new TxnTable();
		}
		return instance_;
	}
	
	public void initServerLog(String dir)
	{
		dir_ = dir;
		String filename = combinePath(dir_, ".serverLog");
		if (Files.exists(Paths.get(filename)))
		{
			readLog(filename);
		}	
		
		serverLog_ = new File(filename);
		try {
			serverOut_ = new FileOutputStream(serverLog_, true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private void readLog(String filename)
	{	
		HashMap<String, String> txnTable = new HashMap<String, String>();
		try {
			byte content[] = Files.readAllBytes(Paths.get(filename));
			String str = new String(content, "UTF-8");
			String[] list = str.split("\t\t");
			for (int i = 0; i < list.length; i++)
			{
				String[] item = list[i].split(" ");
				txnTable.put(item[0], item[1]);	
			}
		} catch (IOException e) {
			e.printStackTrace();
		}	
		if (!txnTable.isEmpty())
		{
			for (String txnId : txnTable.keySet())
			{
				if (txnTable.get(txnId).equals("COMMITTED"))
				{
					TxnData td = new TxnData(txnId, State.COMMITTED);
					table_.put(txnId, td);
				}
				else if (txnTable.get(txnId).equals("ABORTTED"))
				{
					TxnData td = new TxnData(txnId, State.ABORTTED);
					table_.put(txnId, td);
				}
				else
				{
					State state = State.valueOf(txnTable.get(txnId));
					readTxnLog(txnId, state);
				}
				s_txnId = Math.max(Integer.parseInt(txnId), s_txnId);
			}
			s_txnId++;
		}
		
	}
	
	
	private void readTxnLog(String txnId, State state) {
			
		byte content[];
		try {
			String s = "." + txnId;
			String filename = combinePath(dir_, s);
			content = Files.readAllBytes(Paths.get(filename));
			String str = new String(content, "UTF-8");
			String[] list = str.split(delim2);
			TxnData td = null;
			for (int i = 0; i < list.length; i++)
			{
				RequestHandler.RequestMessage rm = new RequestHandler.RequestMessage();
				String[] msg = list[i].split(delim1);
				if(msg != null && msg.length != 0){
					rm.method = msg[0];
					rm.txn_id = msg[1];
					rm.seq_num = msg[2];
					rm.length = msg[3];
					if (msg.length > 4)
					{
						rm.data = msg[4];
					}
					if (rm.method.equals("NEW_TXN"))
					{
						td = new TxnData(txnId, rm.data);
					}
					else if (rm.method.equals("COMMIT"))
					{
						td.recoverTotal_ = rm.seq_num;
					}
					else 
					{
						td.seqSet_.add(rm.seq_num);
						td.reqQueue_.add(rm);
					}					
				}
				
			}
			
			td.state_ = state;
			
			if (td.recoverTotal_ != null)
			{
				td.missSeqSet_ = new HashSet<String>();
				Collections.sort(td.reqQueue_, sortBySeqNum());
				int offset = 1;
				int i = 0;
				for (; i < td.reqQueue_.size(); i++)
				{
					int seq = Integer.parseInt(td.reqQueue_.get(i).seq_num);
					int dif = seq - offset - i;
					if (dif != 0){
						for (int j = seq - dif; j < seq; j++){
							String seqNum = Integer.toString(j);
							td.missSeqSet_.add(seqNum);
						}
					}
					offset += dif;
				} 
				
				i = i + offset;
				
				while (i <= Integer.parseInt(td.recoverTotal_))
				{
					String seqNum = Integer.toString(i);
					td.missSeqSet_.add(seqNum);
					i++;
				}
				td.state_ = State.RESENDING;
			}
			
			td.txnLog_ = new File(filename);
			td.txnOut_ = new FileOutputStream(td.txnLog_, true);
			table_.put(txnId, td);
			startTimer(txnId, RequestHandler.kTimeOutSecs);
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public boolean isValidTxn(String txnId)
	{
		return (table_.containsKey(txnId));
	}	
	
	public synchronized String addNewTxn(RequestHandler.RequestMessage req)
	{
		String txnId = String.valueOf(s_txnId++); 
		TxnData td = new TxnData(txnId, req.data);
		String filename = combinePath(dir_, "." + txnId);
		td.txnLog_ = new File(filename);
		try {
			td.txnOut_ = new FileOutputStream(td.txnLog_, true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		table_.put(txnId, td);
		updateServerLog(txnId, State.UNCOMMITTED);
		updateTxnLog(txnId,req);
		return txnId;
	}
	

	public synchronized void addNewRequest(RequestHandler.RequestMessage request)
	{		
		TxnData td = table_.get(request.txn_id);
		String seqNum = request.seq_num;		
		
		if (!td.missSeqSet_.isEmpty())
		{
			// Resending mode
			if (td.missSeqSet_.contains(seqNum))
			{
				td.reqQueue_.add(request);
				td.missSeqSet_.remove(seqNum);
			}
		}
		else
		{
			if (!td.seqSet_.contains(seqNum))
			{
				td.seqSet_.add(seqNum);
				td.reqQueue_.add(request);
				updateTxnLog(request.txn_id, request);
			}			
		}		
	}
	
	public synchronized void updateState(String txnId, State state)
	{
		TxnData td = table_.get(txnId);
		td.state_ = state;
	}
	
	public synchronized void updateServerLog(String txnId, State state)
	{
		String log = txnId + " " + state.name() + delim1;
		try {
			serverOut_.write(log.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public synchronized void updateTxnLog(String txnId, RequestHandler.RequestMessage req)
	{
		TxnData td = table_.get(txnId);
		String log = req.method + delim1 + req.txn_id + delim1
					+ req.seq_num + delim1 + req.length + delim1
					+ req.data + delim2;
		try {
			td.txnOut_.write(log.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public synchronized void deleteTxnLog(String txnId)
	{
		TxnData td = table_.get(txnId);
		td.txnLog_.delete();
	}
	
	public synchronized void getAllRequests(String txnId, List<RequestHandler.RequestMessage> requests)
	{
		TxnData td = table_.get(txnId);
		requests.addAll(td.reqQueue_);
	}
	
	public State getState(String txnId)
	{
		TxnData td = table_.get(txnId);
		return td.state_;
	}
	
	public synchronized void getMissSeq(String txnId, HashSet<String> missSeq)
	{
		TxnData td = table_.get(txnId);
		missSeq.addAll(td.missSeqSet_);
	}
	
	public synchronized boolean isReadyCommit(String txnId, int totalWrites)
	{
		TxnData td = table_.get(txnId);
		return (td.reqQueue_.size() == totalWrites);
	}
	
	public String getFilePath(String txnId)
	{
		TxnData td = table_.get(txnId);
		return combinePath(dir_, td.filePath_);
	}
	
	public synchronized void addMissingPacket(String txnId, String seqNum)
	{
		TxnData td = table_.get(txnId);
		td.missSeqSet_.add(seqNum);
	}
	
	public synchronized int getNumMissingPackets(String txnId)
	{
		TxnData td = table_.get(txnId);
		return td.missSeqSet_.size();
	}
	
	public synchronized void startTimer(String txnId, int timeInSecs)
	{
		TxnData td = table_.get(txnId);
		td.startTimer(timeInSecs);
	}
	
	public synchronized void cancelTimer(String txnId)
	{
		TxnData td = table_.get(txnId);
		td.cancelTimer();
	}
	
	public synchronized void cleanUp()
	{
		if (serverLog_ != null)
		{
			serverLog_.delete();
		}
		for (String txnId : table_.keySet())
		{
			TxnData td = table_.get(txnId);
			if (td.txnLog_ != null)
			{
				td.txnLog_.delete();
			}
		}
	}
	
	public int getNumRequests(String txnId)
	{
		return table_.get(txnId).reqQueue_.size();
	}
	
	private static Comparator<RequestHandler.RequestMessage> sortBySeqNum()
	{
		Comparator<RequestHandler.RequestMessage> comp = new Comparator<RequestHandler.RequestMessage>(){
			@Override
			public int compare(RequestHandler.RequestMessage rm1, RequestHandler.RequestMessage rm2)
			{
				int seq1 = Integer.parseInt(rm1.seq_num);
				int seq2 = Integer.parseInt(rm2.seq_num);
				return (seq1 > seq2) ? 1 : -1;
			}
		};
		return comp;
	}
	
	public static String combinePath(String dir, String filename)
	{
		File f1 = new File(dir);
		File f2 = new File(f1, filename);
		return f2.getPath();
	}
	
}
